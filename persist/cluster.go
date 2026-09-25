package persist

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// Peer is one voter in the Raft group.
type Peer struct {
	ID      string
	Address string
}

// ClusterOptions configures a single-primary Raft group for one Badger
// directory. Leave Transport nil to listen on Bind. Tests pass an in-memory
// transport and set Advertise to that transport's address.
type ClusterOptions struct {
	ID         string
	Bind       string
	Advertise  string
	RaftDir    string
	Peers      []Peer
	Bootstrap  bool
	ServerUUID string
	Transport  raft.Transport
	Config     *raft.Config
	// ApplyTimeout bounds how long a SQL commit waits for the Raft quorum.
	ApplyTimeout time.Duration
}

// ParsePeers parses "id=host:port,id2=host:port". An empty string is no peers.
func ParsePeers(raw string) ([]Peer, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var peers []Peer
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		id, addr, ok := strings.Cut(part, "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("persist: peer %q must be id=host:port", part)
		}
		peers = append(peers, Peer{ID: id, Address: addr})
	}
	return peers, nil
}

// OpenCluster opens a Badger directory and joins or bootstraps a Raft group
// that replicates its commits. The Raft log is stored beside the Badger
// directory, not inside it.
func OpenCluster(path string, opts ClusterOptions) (*Store, error) {
	if opts.ID == "" {
		return nil, fmt.Errorf("persist: cluster node id is empty")
	}
	if opts.Advertise == "" {
		opts.Advertise = opts.Bind
	}
	if opts.Transport == nil && opts.Bind == "" {
		return nil, fmt.Errorf("persist: cluster bind address is empty")
	}
	if opts.RaftDir == "" {
		opts.RaftDir = filepath.Join(filepath.Dir(path), "raft-"+opts.ID)
	}
	if opts.ApplyTimeout <= 0 {
		opts.ApplyTimeout = 10 * time.Second
	}
	serverUUID, err := loadServerUUID(opts.RaftDir, opts.ServerUUID)
	if err != nil {
		return nil, err
	}
	opts.ServerUUID = serverUUID

	store, err := Open(path)
	if err != nil {
		return nil, err
	}
	bin, err := openBinlog(filepath.Join(opts.RaftDir, "binlog"), opts.ServerUUID)
	if err != nil {
		store.Close()
		return nil, err
	}
	store.bin = bin

	c, err := startCluster(store, opts)
	if err != nil {
		bin.close()
		store.bin = nil
		store.Close()
		return nil, err
	}
	store.cluster = c
	return store, nil
}

func loadServerUUID(dir, id string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	path := filepath.Join(dir, "server.uuid")
	if id == "" {
		raw, err := os.ReadFile(path)
		if err == nil {
			id = strings.TrimSpace(string(raw))
		}
	}
	if id == "" {
		id = uuid.NewString()
	}
	if _, err := uuid.Parse(id); err != nil {
		return "", fmt.Errorf("persist: server uuid: %w", err)
	}
	if err := os.WriteFile(path, []byte(id+"\n"), 0o644); err != nil {
		return "", err
	}
	return id, nil
}

type cluster struct {
	store     *Store
	raft      *raft.Raft
	log       *raftboltdb.BoltStore
	transport raft.Transport
	timeout   time.Duration
}

func startCluster(store *Store, opts ClusterOptions) (*cluster, error) {
	cfg := opts.Config
	if cfg == nil {
		cfg = raft.DefaultConfig()
		cfg.LogLevel = "ERROR"
	}
	cfg.LocalID = raft.ServerID(opts.ID)

	transport := opts.Transport
	var err error
	if transport == nil {
		var addr *net.TCPAddr
		addr, err = net.ResolveTCPAddr("tcp", opts.Advertise)
		if err != nil {
			return nil, err
		}
		transport, err = raft.NewTCPTransport(opts.Bind, addr, 3, 10*time.Second, os.Stderr)
		if err != nil {
			return nil, err
		}
	}

	if err := os.MkdirAll(opts.RaftDir, 0o755); err != nil {
		closeTransport(transport)
		return nil, err
	}
	bolt, err := raftboltdb.NewBoltStore(filepath.Join(opts.RaftDir, "raft.db"))
	if err != nil {
		closeTransport(transport)
		return nil, err
	}
	snaps, err := raft.NewFileSnapshotStore(opts.RaftDir, 2, os.Stderr)
	if err != nil {
		bolt.Close()
		closeTransport(transport)
		return nil, err
	}

	if opts.Bootstrap {
		has, err := raft.HasExistingState(bolt, bolt, snaps)
		if err != nil {
			bolt.Close()
			closeTransport(transport)
			return nil, err
		}
		if !has {
			err = raft.BootstrapCluster(cfg, bolt, bolt, snaps, transport, raft.Configuration{
				Servers: opts.servers(),
			})
			if err != nil {
				bolt.Close()
				closeTransport(transport)
				return nil, err
			}
		}
	}

	r, err := raft.NewRaft(cfg, &storeFSM{store: store}, bolt, bolt, snaps, transport)
	if err != nil {
		bolt.Close()
		closeTransport(transport)
		return nil, err
	}
	return &cluster{
		store:     store,
		raft:      r,
		log:       bolt,
		transport: transport,
		timeout:   opts.ApplyTimeout,
	}, nil
}

func (o ClusterOptions) servers() []raft.Server {
	seen := map[string]bool{}
	var out []raft.Server
	add := func(id, addr string) {
		if id == "" || addr == "" || seen[id] {
			return
		}
		seen[id] = true
		out = append(out, raft.Server{
			ID:       raft.ServerID(id),
			Address:  raft.ServerAddress(addr),
			Suffrage: raft.Voter,
		})
	}
	add(o.ID, o.Advertise)
	for _, peer := range o.Peers {
		add(peer.ID, peer.Address)
	}
	return out
}

func (c *cluster) propose(batch replBatch) error {
	payload, err := encodeBatch(batch)
	if err != nil {
		return err
	}
	future := c.raft.Apply(payload, c.timeout)
	if err := future.Error(); err != nil {
		return err
	}
	if resp, ok := future.Response().(error); ok && resp != nil {
		return resp
	}
	return nil
}

func (c *cluster) notLeader() error {
	addr := c.raft.Leader()
	if addr == "" {
		return fmt.Errorf("persist: not the leader; leader is unknown")
	}
	return fmt.Errorf("persist: not the leader; leader is %s", addr)
}

func (c *cluster) shutdown() error {
	if c.raft != nil {
		if err := c.raft.Shutdown().Error(); err != nil {
			return err
		}
	}
	closeTransport(c.transport)
	if c.log != nil {
		return c.log.Close()
	}
	return nil
}

// IsLeader reports whether this process currently accepts SQL writes.
func (s *Store) IsLeader() bool {
	return s.cluster != nil && s.cluster.raft.State() == raft.Leader
}

// Replicating reports whether commits go through Raft.
func (s *Store) Replicating() bool {
	return s.cluster != nil
}

// WaitReady waits until this node is the leader. A bootstrap node calls it
// before serving writes.
func (s *Store) WaitReady(timeout time.Duration) error {
	if s.cluster == nil {
		return nil
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.IsLeader() {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("persist: timed out waiting for leadership")
}

// AddVoter adds a replica that is already running at addr.
func (s *Store) AddVoter(id, addr string) error {
	if s.cluster == nil {
		return fmt.Errorf("persist: store is not replicating")
	}
	return s.cluster.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, s.cluster.timeout).Error()
}

// Snapshot asks Raft to compact the log into a Badger backup. A replica that
// joins after the log is truncated installs that backup.
func (s *Store) Snapshot() error {
	if s.cluster == nil {
		return fmt.Errorf("persist: store is not replicating")
	}
	return s.cluster.raft.Snapshot().Error()
}

// TransferLeadership moves the primary to another voter.
func (s *Store) TransferLeadership() error {
	if s.cluster == nil {
		return fmt.Errorf("persist: store is not replicating")
	}
	return s.cluster.raft.LeadershipTransfer().Error()
}

// LastIndex is the newest Raft log index, including configuration entries.
func (s *Store) LastIndex() (uint64, error) {
	if s.cluster == nil {
		return 0, fmt.Errorf("persist: store is not replicating")
	}
	return s.cluster.raft.LastIndex(), nil
}

func closeTransport(transport raft.Transport) {
	closer, ok := transport.(io.Closer)
	if ok && closer != nil {
		_ = closer.Close()
	}
}

// AppliedIndex is the newest index applied into this node's Badger.
func (s *Store) AppliedIndex() uint64 {
	if s.cluster == nil {
		return 0
	}
	return s.cluster.raft.AppliedIndex()
}
