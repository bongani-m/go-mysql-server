package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/vt/proto/query"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/eventscheduler"
	"github.com/dolthub/go-mysql-server/persist"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// Persistent MySQL server for the example people table. Rows live in a Badger
// directory and are still there after this process exits.
//
//	GMS_BOOTSTRAP_PASSWORD=secret go run ./_persist
//	mysql --host=127.0.0.1 --port=3306 --user=root --password=secret mydb --execute="SELECT name, email FROM mytable;"
//
// The HTTP API in _example_webapp connects to this server unchanged.
// Set GMS_DATA to choose the directory. The default is data/gms.
//
// Cluster mode is off unless GMS_RAFT_ADDR is set. One node bootstraps:
//
//	GMS_NODE_ID=n1 GMS_RAFT_ADDR=127.0.0.1:7001 GMS_RAFT_BOOTSTRAP=1 \
//	  GMS_SERVER_UUID=11111111-1111-1111-1111-111111111111 \
//	  GMS_RAFT_PEERS=n1=127.0.0.1:7001,n2=127.0.0.1:7002 go run ./_persist
//
// Other nodes use the same GMS_SERVER_UUID and GMS_RAFT_PEERS, their own
// GMS_NODE_ID and GMS_RAFT_ADDR, and leave GMS_RAFT_BOOTSTRAP unset. Start
// them with the bootstrap node so the group can elect a leader.
//
//	docker compose -f _persist/compose.yaml up --build
//
// Then, from the host:
//
//	mysql --host=127.0.0.1 --port=3306 --user=root --password=secret mydb --execute="SELECT name, email FROM mytable;"
//	mysql --host=127.0.0.1 --port=3307 --user=root --password=secret mydb --execute="SELECT name, email FROM mytable;"
//
// GMS_MYSQL_HOST defaults to localhost. Set it to 0.0.0.0 to accept connections
// from other containers and from published host ports. GMS_MYSQL_PORT overrides
// 3306. GMS_RAFT_ADVERTISE is the address other nodes dial; GMS_RAFT_ADDR is
// the address this process binds.

var (
	dbName    = "mydb"
	tableName = "mytable"
	address   = "localhost"
	port      = 3306
)

func main() {
	if host := os.Getenv("GMS_MYSQL_HOST"); host != "" {
		address = host
	}
	if raw := os.Getenv("GMS_MYSQL_PORT"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 65535 {
			log.Fatalf("GMS_MYSQL_PORT: %q", raw)
		}
		port = parsed
	}
	path := os.Getenv("GMS_DATA")
	if path == "" {
		path = "data/gms"
	}
	gate := &leadershipGate{}
	var leader *leaderExec
	store, err := openStore(path, func(isLeader bool) {
		gate.set(isLeader)
		if !isLeader && leader != nil {
			leader.dropAll()
		}
	})
	if err != nil {
		log.Fatalf("open %s: %v", path, err)
	}
	defer store.Close()

	ctx := sql.NewContext(context.Background())
	if !store.Replicating() || store.IsLeader() {
		if err := ensureExample(ctx, store); err != nil {
			log.Fatalf("seed %s.%s: %v", dbName, tableName, err)
		}
	}

	engine := sqle.NewDefault(store)
	if err := engine.InitializeEventScheduler(func() (*sql.Context, error) {
		return sql.NewContext(context.Background(), sql.WithSession(persist.NewSession(sql.NewBaseSession(), store))), nil
	}, eventscheduler.SchedulerOn, 0); err != nil {
		log.Fatalf("events: %v", err)
	}
	if store.Replicating() {
		engine.Analyzer.Catalog.BinlogPrimaryController = store
		engine.Analyzer.Catalog.BinlogReplicaController = store
		store.SetReplicaQuery(func(ctx *sql.Context, query string) error {
			_, iter, _, err := engine.Query(ctx, query)
			if err != nil {
				return err
			}
			_, err = sql.RowIterToRows(ctx, iter)
			return err
		})
	}
	gate.bind(engine, store.Replicating(), store.IsLeader())
	if store.Replicating() {
		leader = newLeaderExec(engine, store)
		store.SetForwardExec(leader.Exec)
	}
	if err := enableAuth(ctx, store, engine, accountFromEnv()); err != nil {
		log.Fatalf("auth: %v", err)
	}
	if err := enableUpstream(store); err != nil {
		log.Fatalf("upstream replica: %v", err)
	}
	tlsConfig, err := loadServerTLS(os.Getenv("GMS_TLS_CERT"), os.Getenv("GMS_TLS_KEY"))
	if err != nil {
		log.Fatalf("tls: %v", err)
	}
	config := server.Config{
		Protocol:               "tcp",
		Address:                fmt.Sprintf("%s:%d", address, port),
		TLSConfig:              tlsConfig,
		RequireSecureTransport: tlsConfig != nil,
	}
	var s *server.Server
	if store.Replicating() {
		s, err = server.NewServerWithHandler(config, engine, sql.NewContext, persist.NewSessionBuilder(store), nil, func(h mysql.Handler) (mysql.Handler, error) {
			inner, ok := h.(*server.Handler)
			if !ok {
				return nil, fmt.Errorf("persist: unexpected mysql handler %T", h)
			}
			return newForwardHandler(inner, store), nil
		})
	} else {
		s, err = server.NewServer(config, engine, sql.NewContext, persist.NewSessionBuilder(store), nil)
	}
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("persistent MySQL listening on %s, data file %s", config.Address, path)
	if err = s.Start(); err != nil {
		log.Fatal(err)
	}
}

// leadershipGate turns the SQL engine read-only while this process is not
// the Raft primary. The engine does not exist until after the store opens.
type leadershipGate struct {
	mu     sync.Mutex
	engine *sqle.Engine
}

func (g *leadershipGate) set(isLeader bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.engine != nil {
		g.engine.ReadOnly.Store(!isLeader)
	}
}

func (g *leadershipGate) bind(engine *sqle.Engine, replicating, leader bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.engine = engine
	if replicating {
		engine.ReadOnly.Store(!leader)
	}
}

func openStore(path string, onLeadership func(bool)) (*persist.Store, error) {
	addr := os.Getenv("GMS_RAFT_ADDR")
	if addr == "" {
		return persist.Open(path)
	}
	id := os.Getenv("GMS_NODE_ID")
	if id == "" {
		id = addr
	}
	peers, err := persist.ParsePeers(os.Getenv("GMS_RAFT_PEERS"))
	if err != nil {
		return nil, err
	}
	bootstrap := os.Getenv("GMS_RAFT_BOOTSTRAP") == "1" || strings.EqualFold(os.Getenv("GMS_RAFT_BOOTSTRAP"), "true")
	maxBytes, err := parseBinlogMax(os.Getenv("GMS_BINLOG_MAX_SIZE"))
	if err != nil {
		return nil, err
	}
	var tlsConfig *tls.Config
	if os.Getenv("GMS_RAFT_TLS_CERT") != "" || os.Getenv("GMS_RAFT_TLS_KEY") != "" || os.Getenv("GMS_RAFT_TLS_CA") != "" {
		tlsConfig, err = persist.LoadRaftTLS(os.Getenv("GMS_RAFT_TLS_CERT"), os.Getenv("GMS_RAFT_TLS_KEY"), os.Getenv("GMS_RAFT_TLS_CA"))
		if err != nil {
			return nil, err
		}
	}
	store, err := persist.OpenCluster(path, persist.ClusterOptions{
		ID:             id,
		Bind:           addr,
		Advertise:      os.Getenv("GMS_RAFT_ADVERTISE"),
		RaftDir:        os.Getenv("GMS_RAFT_DIR"),
		Peers:          peers,
		Bootstrap:      bootstrap,
		ServerUUID:     os.Getenv("GMS_SERVER_UUID"),
		BinlogMaxBytes: maxBytes,
		OnLeadership:   onLeadership,
		TLS:            tlsConfig,
		ForwardAddr:    os.Getenv("GMS_FORWARD_ADDR"),
	})
	if err != nil {
		return nil, err
	}
	if bootstrap {
		if err := store.WaitReady(30 * time.Second); err != nil {
			store.Close()
			return nil, err
		}
	}
	return store, nil
}

func parseBinlogMax(raw string) (uint64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("GMS_BINLOG_MAX_SIZE: %q", raw)
	}
	return n, nil
}

func enableUpstream(store *persist.Store) error {
	host := strings.TrimSpace(os.Getenv("GMS_SOURCE_HOST"))
	if host == "" {
		return nil
	}
	port := 3306
	if raw := os.Getenv("GMS_SOURCE_PORT"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 65535 {
			return fmt.Errorf("GMS_SOURCE_PORT: %q", raw)
		}
		port = parsed
	}
	store.EnableUpstream(host, uint16(port), os.Getenv("GMS_SOURCE_USER"), os.Getenv("GMS_SOURCE_PASSWORD"))
	return nil
}

func ensureExample(ctx *sql.Context, store *persist.Store) error {
	if !store.HasDatabase(ctx, dbName) {
		if err := store.CreateDatabase(ctx, dbName); err != nil {
			return err
		}
	}
	db, err := store.Database(ctx, dbName)
	if err != nil {
		return err
	}
	_, ok, err := db.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	if err := db.(sql.TableCreator).CreateTable(ctx, tableName, peopleSchema(), sql.Collation_Default, ""); err != nil {
		return err
	}
	table, ok, err := db.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("table %s was not created", tableName)
	}

	inserter := table.(sql.InsertableTable).Inserter(ctx)
	inserter.StatementBegin(ctx)
	created := time.Unix(0, 1667304000000001000).UTC()
	for _, person := range seedPeople {
		err := inserter.Insert(ctx, sql.NewRow(
			person.id,
			person.name,
			person.email,
			types.MustJSON(person.phones),
			created,
		))
		if err != nil {
			_ = inserter.DiscardChanges(ctx, err)
			_ = inserter.Close(ctx)
			return err
		}
	}
	if err := inserter.StatementComplete(ctx); err != nil {
		return err
	}
	return inserter.Close(ctx)
}

func peopleSchema() sql.PrimaryKeySchema {
	return sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "id", Type: types.Int64, Nullable: false, Source: tableName, PrimaryKey: true, AutoIncrement: true},
		{Name: "name", Type: types.Text, Nullable: false, Source: tableName},
		{Name: "email", Type: types.Text, Nullable: false, Source: tableName},
		{Name: "phone_numbers", Type: types.JSON, Nullable: false, Source: tableName},
		{Name: "created_at", Type: types.MustCreateDatetimeType(query.Type_DATETIME, 6), Nullable: false, Source: tableName},
	})
}

var seedPeople = []struct {
	id     int64
	name   string
	email  string
	phones string
}{
	{1, "Jane Deo", "janedeo@gmail.com", `["556-565-566","777-777-777"]`},
	{2, "Jane Doe", "jane@doe.com", `[]`},
	{3, "John Doe", "john@doe.com", `["555-555-555"]`},
	{4, "John Doe", "johnalt@doe.com", `[]`},
}
