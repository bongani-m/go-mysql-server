package persist

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// pendingCap is how many recorded batches may wait for Raft at once.
// Past that, commit waits until an apply frees a slot.
const pendingCap = 32

var errClusterClosed = errors.New("persist: cluster shut down")

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

// queuedCommit is one recorded statement waiting for its Raft result.
type queuedCommit struct {
	id    uint64
	batch replBatch
	done  chan error
}

type cluster struct {
	store     *Store
	raft      *raft.Raft
	log       *raftboltdb.BoltStore
	transport raft.Transport
	timeout   time.Duration

	// recordMu serializes recording. It is not held while Raft waits, so the
	// next statement can record against batches still in flight.
	recordMu sync.Mutex
	cond     *sync.Cond
	inflight []*queuedCommit
	queue    []*queuedCommit
	nextID   uint64
	stopped  bool
	exited   chan struct{}
	// gate, if set, runs on the proposer before each group is sent to Raft.
	// Tests hold it to observe a commit that is recorded but not yet applied.
	gate func()
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
	c := &cluster{
		store:     store,
		raft:      r,
		log:       bolt,
		transport: transport,
		timeout:   opts.ApplyTimeout,
		exited:    make(chan struct{}),
		nextID:    newBatchEpoch(),
	}
	c.cond = sync.NewCond(&store.mu)
	go c.proposeLoop()
	return c, nil
}

// newBatchEpoch keeps in-flight ids from matching commands replayed out of an
// older log. noteApplied ignores id 0, so an epoch of 0 becomes 1.
func newBatchEpoch() uint64 {
	var seed uint64
	if err := binary.Read(rand.Reader, binary.LittleEndian, &seed); err != nil || seed == 0 {
		return 1
	}
	return seed
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

// beginRecord waits for a free in-flight slot and returns the writes the next
// statement must see. The caller holds recordMu and does not hold the store lock.
func (c *cluster) beginRecord() ([]kvOp, error) {
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	for {
		if c.stopped {
			return nil, errClusterClosed
		}
		if c.raft.State() != raft.Leader {
			return nil, c.notLeader()
		}
		if len(c.inflight) < pendingCap {
			break
		}
		c.cond.Wait()
	}
	return snapshotOps(c.inflight), nil
}

func snapshotOps(pending []*queuedCommit) []kvOp {
	n := 0
	for _, q := range pending {
		n += len(q.batch.Ops)
	}
	ops := make([]kvOp, 0, n)
	for _, q := range pending {
		ops = append(ops, q.batch.Ops...)
	}
	return ops
}

// enqueue assigns a batch id and hands it to the proposer. The caller holds recordMu.
func (c *cluster) enqueue(batch replBatch) (chan error, error) {
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	if c.stopped {
		return nil, errClusterClosed
	}
	if c.raft.State() != raft.Leader {
		return nil, c.notLeader()
	}
	c.nextID++
	if c.nextID == 0 {
		c.nextID = 1
	}
	batch.ID = c.nextID
	q := &queuedCommit{
		id:    batch.ID,
		batch: batch,
		done:  make(chan error, 1),
	}
	c.inflight = append(c.inflight, q)
	c.queue = append(c.queue, q)
	c.cond.Broadcast()
	return q.done, nil
}

// proposeLoop sends each recorded group to Raft in record order, and only then
// waits. Raft coalesces those entries into one AppendEntries round trip.
func (c *cluster) proposeLoop() {
	defer close(c.exited)
	for {
		group := c.takeQueue()
		if group == nil {
			return
		}
		c.proposeGroup(group)
	}
}

func (c *cluster) takeQueue() []*queuedCommit {
	c.store.mu.Lock()
	for len(c.queue) == 0 && !c.stopped {
		c.cond.Wait()
	}
	if c.stopped {
		queued := c.detachQueuedLocked()
		c.store.mu.Unlock()
		for _, q := range queued {
			q.done <- errClusterClosed
		}
		return nil
	}
	group := c.queue
	c.queue = nil
	c.store.mu.Unlock()
	return group
}

func (c *cluster) proposeGroup(group []*queuedCommit) {
	if c.gate != nil {
		c.gate()
	}
	futures := make([]raft.ApplyFuture, len(group))
	for i, q := range group {
		payload, err := encodeBatch(q.batch)
		if err != nil {
			c.finishProposed(group, futures, i, err)
			return
		}
		futures[i] = c.raft.Apply(payload, c.timeout)
	}
	c.finishProposed(group, futures, len(group), nil)
}

// finishProposed waits for futures that were sent. A Raft error from index
// failAt, or an encode error there, fails that batch and every later one,
// including batches recorded after this group.
func (c *cluster) finishProposed(group []*queuedCommit, futures []raft.ApplyFuture, failAt int, failErr error) {
	results := make([]error, len(group))
	firstRaftFail := failAt
	if failAt < len(group) && failErr != nil {
		results[failAt] = failErr
	}
	for i := 0; i < len(group) && i < failAt; i++ {
		err := futures[i].Error()
		if err != nil {
			results[i] = err
			if firstRaftFail > i {
				firstRaftFail = i
				failErr = err
			}
			continue
		}
		if resp, ok := futures[i].Response().(error); ok && resp != nil {
			results[i] = resp
		}
	}
	if firstRaftFail < len(group) {
		notified := make(map[uint64]bool, len(group)-firstRaftFail)
		for i := firstRaftFail; i < len(group); i++ {
			notified[group[i].id] = true
			err := results[i]
			if err == nil {
				err = failErr
			}
			group[i].done <- err
		}
		c.abandonFrom(group[firstRaftFail].id, failErr, notified)
		for i := 0; i < firstRaftFail; i++ {
			group[i].done <- results[i]
		}
		return
	}
	for i, q := range group {
		q.done <- results[i]
	}
}

// abandonFrom drops id and every in-flight batch recorded after it.
// alreadyNotified batches are left for the caller to wake.
func (c *cluster) abandonFrom(id uint64, err error, alreadyNotified map[uint64]bool) {
	var dropped []*queuedCommit
	c.store.mu.Lock()
	drop := false
	kept := make([]*queuedCommit, 0, len(c.inflight))
	for _, q := range c.inflight {
		if q.id == id {
			drop = true
		}
		if drop {
			dropped = append(dropped, q)
			continue
		}
		kept = append(kept, q)
	}
	if drop {
		c.inflight = kept
		skip := make(map[uint64]bool, len(dropped))
		for _, q := range dropped {
			skip[q.id] = true
		}
		queued := make([]*queuedCommit, 0, len(c.queue))
		for _, q := range c.queue {
			if !skip[q.id] {
				queued = append(queued, q)
			}
		}
		c.queue = queued
		c.cond.Broadcast()
	}
	c.store.mu.Unlock()
	for _, q := range dropped {
		if alreadyNotified[q.id] {
			continue
		}
		q.done <- err
	}
}

// detachQueuedLocked removes batches still waiting to be proposed. The store lock is held.
func (c *cluster) detachQueuedLocked() []*queuedCommit {
	queued := c.queue
	c.queue = nil
	if len(queued) == 0 {
		return nil
	}
	skip := make(map[uint64]bool, len(queued))
	for _, q := range queued {
		skip[q.id] = true
	}
	kept := make([]*queuedCommit, 0, len(c.inflight))
	for _, q := range c.inflight {
		if !skip[q.id] {
			kept = append(kept, q)
		}
	}
	c.inflight = kept
	c.cond.Broadcast()
	return queued
}

// noteApplied drops the leader's in-flight batch once its writes are in Badger.
// id 0 is a log entry that this process did not propose.
func (s *Store) noteApplied(id uint64) {
	if id == 0 || s.cluster == nil {
		return
	}
	c := s.cluster
	s.mu.Lock()
	for i, q := range c.inflight {
		if q.id != id {
			continue
		}
		copy(c.inflight[i:], c.inflight[i+1:])
		c.inflight[len(c.inflight)-1] = nil
		c.inflight = c.inflight[:len(c.inflight)-1]
		c.cond.Broadcast()
		break
	}
	s.mu.Unlock()
}

func (c *cluster) notLeader() error {
	addr := c.raft.Leader()
	if addr == "" {
		return fmt.Errorf("persist: not the leader; leader is unknown")
	}
	return fmt.Errorf("persist: not the leader; leader is %s", addr)
}

func (c *cluster) shutdown() error {
	c.store.mu.Lock()
	c.stopped = true
	c.cond.Broadcast()
	c.store.mu.Unlock()
	var err error
	if c.raft != nil {
		err = c.raft.Shutdown().Error()
	}
	<-c.exited
	closeTransport(c.transport)
	if c.log != nil {
		if cerr := c.log.Close(); err == nil {
			err = cerr
		}
	}
	return err
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
