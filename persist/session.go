package persist

import (
	"context"
	"sync"

	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
)

// Session tracks an uncommitted transaction. Autocommit statements write in
// editor.Close. BEGIN keeps those edits here until COMMIT or ROLLBACK.
type Session struct {
	*sql.BaseSession
	store   *Store
	mu      sync.Mutex
	pending map[tableRef][]edit
}

var _ sql.Session = (*Session)(nil)
var _ sql.TransactionSession = (*Session)(nil)

// NewSession returns a session that commits into store.
func NewSession(base *sql.BaseSession, store *Store) *Session {
	return &Session{
		BaseSession: base,
		store:       store,
		pending:     make(map[tableRef][]edit),
	}
}

// NewSessionBuilder builds sessions for server.NewServer.
func NewSessionBuilder(store *Store) func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
	return func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
		host := ""
		user := ""
		mysqlUser, ok := conn.UserData.(sql.MysqlConnectionUser)
		if ok {
			host = mysqlUser.Host
			user = mysqlUser.User
		}
		client := sql.Client{Address: host, User: user, Capabilities: conn.Capabilities}
		base := sql.NewBaseSessionWithClientServer(addr, client, conn.ConnectionID)
		return NewSession(base, store), nil
	}
}

type transaction struct {
	readOnly bool
}

func (t *transaction) String() string {
	if t.readOnly {
		return "badger read-only transaction"
	}
	return "badger transaction"
}

func (t *transaction) IsReadOnly() bool { return t.readOnly }

// StartTransaction implements sql.TransactionSession.
func (s *Session) StartTransaction(ctx *sql.Context, characteristic sql.TransactionCharacteristic) (sql.Transaction, error) {
	return &transaction{readOnly: characteristic == sql.ReadOnly}, nil
}

// CommitTransaction writes every edit buffered since BEGIN.
func (s *Session) CommitTransaction(ctx *sql.Context, tx sql.Transaction) error {
	s.mu.Lock()
	pending := s.pending
	s.pending = make(map[tableRef][]edit)
	s.mu.Unlock()
	if len(pending) == 0 {
		return nil
	}
	if err := s.store.applyAll(pending); err != nil {
		s.mu.Lock()
		s.restore(pending)
		s.mu.Unlock()
		return err
	}
	return nil
}

// Rollback drops edits buffered since BEGIN. Writes that already committed
// (autocommit statements and TRUNCATE) stay on disk.
func (s *Session) Rollback(ctx *sql.Context, transaction sql.Transaction) error {
	s.mu.Lock()
	s.pending = make(map[tableRef][]edit)
	s.mu.Unlock()
	return nil
}

// CreateSavepoint implements sql.TransactionSession. Savepoints are ignored.
func (s *Session) CreateSavepoint(ctx *sql.Context, transaction sql.Transaction, name string) error {
	return nil
}

// RollbackToSavepoint implements sql.TransactionSession. Savepoints are ignored.
func (s *Session) RollbackToSavepoint(ctx *sql.Context, transaction sql.Transaction, name string) error {
	return nil
}

// ReleaseSavepoint implements sql.TransactionSession. Savepoints are ignored.
func (s *Session) ReleaseSavepoint(ctx *sql.Context, transaction sql.Transaction, name string) error {
	return nil
}

func (s *Session) add(ref tableRef, edits []edit) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending[ref] = append(s.pending[ref], edits...)
}

func (s *Session) edits(ref tableRef) []edit {
	s.mu.Lock()
	defer s.mu.Unlock()
	src := s.pending[ref]
	out := make([]edit, len(src))
	copy(out, src)
	return out
}

func (s *Session) clear(ref tableRef) {
	s.mu.Lock()
	delete(s.pending, ref)
	s.mu.Unlock()
}

func (s *Session) rename(from, to tableRef) {
	s.mu.Lock()
	defer s.mu.Unlock()
	edits, ok := s.pending[from]
	if !ok {
		return
	}
	delete(s.pending, from)
	s.pending[to] = append(edits, s.pending[to]...)
}

// restore puts pending edits back in front of anything buffered after a failed commit.
func (s *Session) restore(pending map[tableRef][]edit) {
	for ref, edits := range pending {
		s.pending[ref] = append(edits, s.pending[ref]...)
	}
}
