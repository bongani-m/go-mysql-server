package persist

import (
	"context"
	"sync"

	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
)

// rowImage is a primary key and the on-disk bytes a locking read observed.
type rowImage struct {
	ref tableRef
	key []byte
	raw []byte
}

// savepoint marks how far a transaction had progressed. Rollback truncates
// pending edits, open editors, and locking reads back to these lengths.
type savepoint struct {
	name    string
	pending map[tableRef]int
	editors map[*editor]int
	reads   int
}

// Session tracks an uncommitted transaction. Autocommit statements write in
// editor.Close. BEGIN keeps those edits here until COMMIT or ROLLBACK.
type Session struct {
	*sql.BaseSession
	store   *Store
	mu      sync.Mutex
	pending map[tableRef][]edit
	// open editors have not closed yet. A statement can write one table through
	// more than one editor (INSERT ... ON DUPLICATE KEY UPDATE), and each has
	// to see the others' buffered rows.
	open       map[tableRef][]*editor
	reads      []rowImage
	savepoints []savepoint
	locking    bool
}

var _ sql.Session = (*Session)(nil)
var _ sql.TransactionSession = (*Session)(nil)
var _ sql.LockingReadSession = (*Session)(nil)

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

// SetLockingRead implements sql.LockingReadSession. The next scan in an
// explicit transaction records each on-disk row it yields.
func (s *Session) SetLockingRead(on bool) {
	s.mu.Lock()
	s.locking = on
	s.mu.Unlock()
}

// CommitTransaction writes every edit buffered since BEGIN. A row image that
// no longer matches disk aborts the transaction and drops the buffered edits.
func (s *Session) CommitTransaction(ctx *sql.Context, tx sql.Transaction) error {
	s.mu.Lock()
	pending := s.pending
	reads := s.reads
	points := s.savepoints
	s.pending = make(map[tableRef][]edit)
	s.reads = nil
	s.savepoints = nil
	s.mu.Unlock()
	gtid := sourceGTID(ctx)
	if len(pending) == 0 && len(reads) == 0 && gtid == "" {
		return nil
	}
	statement := ""
	if ctx != nil {
		statement = ctx.Query()
	}
	if err := s.store.applyAll(pending, reads, statement, gtid); err != nil {
		if sql.ErrLockDeadlock.Is(err) {
			return err
		}
		s.mu.Lock()
		s.restore(pending)
		s.reads = append(reads, s.reads...)
		s.savepoints = points
		s.mu.Unlock()
		return err
	}
	return nil
}

// Rollback drops edits buffered since BEGIN. Writes that already committed
// (autocommit statements and TRUNCATE) stay on disk.
func (s *Session) Rollback(ctx *sql.Context, transaction sql.Transaction) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = make(map[tableRef][]edit)
	s.reads = nil
	s.savepoints = nil
	s.clearOpenEditors()
	return nil
}

// CreateSavepoint records the buffered edits and open editors. Repeating a
// name replaces that mark and drops marks recorded after it.
func (s *Session) CreateSavepoint(ctx *sql.Context, transaction sql.Transaction, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if i := s.savepointIndex(name); i >= 0 {
		s.savepoints = s.savepoints[:i]
	}
	s.savepoints = append(s.savepoints, s.capture(name))
	return nil
}

// RollbackToSavepoint truncates edits, open editors, and locking reads back
// to the named mark and drops marks recorded after it.
func (s *Session) RollbackToSavepoint(ctx *sql.Context, transaction sql.Transaction, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.savepointIndex(name)
	if i < 0 {
		return sql.ErrSavepointDoesNotExist.New(name)
	}
	sp := s.savepoints[i]
	s.savepoints = s.savepoints[:i+1]
	s.restorePending(sp.pending)
	s.restoreEditors(sp.editors)
	if sp.reads < len(s.reads) {
		s.reads = s.reads[:sp.reads]
	}
	return nil
}

// ReleaseSavepoint drops the named mark and every mark recorded after it.
// The edits stay.
func (s *Session) ReleaseSavepoint(ctx *sql.Context, transaction sql.Transaction, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := s.savepointIndex(name)
	if i < 0 {
		return sql.ErrSavepointDoesNotExist.New(name)
	}
	s.savepoints = s.savepoints[:i]
	return nil
}

func (s *Session) savepointIndex(name string) int {
	for i, sp := range s.savepoints {
		if sp.name == name {
			return i
		}
	}
	return -1
}

func (s *Session) capture(name string) savepoint {
	pending := make(map[tableRef]int, len(s.pending))
	for ref, edits := range s.pending {
		pending[ref] = len(edits)
	}
	editors := make(map[*editor]int)
	for _, eds := range s.open {
		for _, ed := range eds {
			editors[ed] = len(ed.edits)
		}
	}
	return savepoint{name: name, pending: pending, editors: editors, reads: len(s.reads)}
}

func (s *Session) restorePending(marks map[tableRef]int) {
	for ref, edits := range s.pending {
		n, ok := marks[ref]
		if !ok {
			delete(s.pending, ref)
			continue
		}
		if n < len(edits) {
			s.pending[ref] = edits[:n]
		}
	}
}

func (s *Session) restoreEditors(marks map[*editor]int) {
	for _, eds := range s.open {
		for _, ed := range eds {
			n, ok := marks[ed]
			if !ok {
				ed.edits = nil
				ed.mark = 0
				continue
			}
			if n < len(ed.edits) {
				ed.edits = ed.edits[:n]
			}
			if ed.mark > len(ed.edits) {
				ed.mark = len(ed.edits)
			}
		}
	}
}

func (s *Session) clearOpenEditors() {
	for _, eds := range s.open {
		for _, ed := range eds {
			ed.edits = nil
			ed.mark = 0
		}
	}
}

func (s *Session) noteLockedRead(ctx *sql.Context, ref tableRef, key, raw []byte) {
	if len(raw) == 0 || ctx == nil || !ctx.GetIgnoreAutoCommit() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.locking {
		return
	}
	s.reads = append(s.reads, rowImage{
		ref: ref,
		key: append([]byte(nil), key...),
		raw: append([]byte(nil), raw...),
	})
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

func (s *Session) track(e *editor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.open == nil {
		s.open = make(map[tableRef][]*editor)
	}
	ref := e.table.ref()
	s.open[ref] = append(s.open[ref], e)
}

func (s *Session) untrack(e *editor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ref := e.table.ref()
	eds := s.open[ref]
	for i, ed := range eds {
		if ed != e {
			continue
		}
		s.open[ref] = append(eds[:i], eds[i+1:]...)
		return
	}
}

func (s *Session) openEditsExcept(ref tableRef, self *editor) []edit {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []edit
	for _, ed := range s.open[ref] {
		if ed == self {
			continue
		}
		out = append(out, ed.edits...)
	}
	return out
}

func (s *Session) openEdits(ref tableRef) []edit {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []edit
	for _, ed := range s.open[ref] {
		out = append(out, ed.edits...)
	}
	return out
}

// lookupOpen reports the newest view of key in every open editor except self.
func (s *Session) lookupOpen(self *editor, key []byte) (sql.Row, bool, bool) {
	s.mu.Lock()
	eds := append([]*editor(nil), s.open[self.table.ref()]...)
	s.mu.Unlock()
	var row sql.Row
	var ok, decided bool
	for _, ed := range eds {
		if ed == self {
			continue
		}
		if next, found, hit := lookupEdits(ed.edits, key); hit {
			row, ok, decided = next, found, true
		}
	}
	return row, ok, decided
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
