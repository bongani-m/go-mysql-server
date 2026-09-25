package persist

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestLostUpdate(t *testing.T) {
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	base := sql.NewContext(context.Background())
	table := mustTable(t, base, createPeopleTable(t, base, store))
	orig := personRow(1, "Jane", "jane@b.c")
	require.NoError(t, insertRows(base, table, orig))

	sessA, ctxA := beginSession(t, store)
	sessB, ctxB := beginSession(t, store)
	winner := personRow(1, "Jane", "winner@b.c")
	loser := personRow(1, "Jane", "loser@b.c")
	require.NoError(t, updateRow(ctxA, table, orig, winner))
	require.NoError(t, updateRow(ctxB, table, orig, loser))
	require.NoError(t, sessA.CommitTransaction(ctxA, ctxA.GetTransaction()))

	err := sessB.CommitTransaction(ctxB, ctxB.GetTransaction())
	require.Error(t, err)
	require.True(t, sql.ErrLockDeadlock.Is(err))
	require.Equal(t, "winner@b.c", readRows(t, base, table)[0][2])

	third := personRow(1, "Jane", "third@b.c")
	require.NoError(t, updateRow(ctxB, table, winner, third))
	require.NoError(t, sessB.CommitTransaction(ctxB, ctxB.GetTransaction()))
	require.Equal(t, "third@b.c", readRows(t, base, table)[0][2])
}

func TestSameTransactionRewritesRow(t *testing.T) {
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	base := sql.NewContext(context.Background())
	table := mustTable(t, base, createPeopleTable(t, base, store))
	orig := personRow(1, "Jane", "jane@b.c")
	require.NoError(t, insertRows(base, table, orig))

	sess, ctx := beginSession(t, store)
	mid := personRow(1, "Jane", "mid@b.c")
	final := personRow(1, "Jane", "final@b.c")
	require.NoError(t, updateRow(ctx, table, orig, mid))
	require.NoError(t, updateRow(ctx, table, mid, final))
	require.NoError(t, sess.CommitTransaction(ctx, ctx.GetTransaction()))
	require.Equal(t, "final@b.c", readRows(t, base, table)[0][2])
}

func TestConcurrentAutoIncrement(t *testing.T) {
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	base := sql.NewContext(context.Background())
	table := mustTable(t, base, createPeopleTable(t, base, store))

	const n = 8
	ids := make([]uint64, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ids[i], errs[i] = table.GetNextAutoIncrementValue(sql.NewContext(context.Background()), nil)
		}(i)
	}
	close(start)
	wg.Wait()

	seen := make(map[uint64]struct{}, n)
	for i := 0; i < n; i++ {
		require.NoError(t, errs[i])
		seen[ids[i]] = struct{}{}
	}
	require.Len(t, seen, n)
}

func TestSavepoints(t *testing.T) {
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	base := sql.NewContext(context.Background())
	table := mustTable(t, base, createPeopleTable(t, base, store))
	sess, ctx := beginSession(t, store)
	tx := ctx.GetTransaction()

	require.NoError(t, insertRows(ctx, table, personRow(1, "one", "one@b.c")))
	require.NoError(t, sess.CreateSavepoint(ctx, tx, "a"))
	require.NoError(t, insertRows(ctx, table, personRow(2, "two", "two@b.c")))
	require.NoError(t, sess.CreateSavepoint(ctx, tx, "b"))
	require.NoError(t, insertRows(ctx, table, personRow(3, "three", "three@b.c")))
	require.NoError(t, sess.RollbackToSavepoint(ctx, tx, "a"))
	require.Error(t, sess.RollbackToSavepoint(ctx, tx, "b"))
	require.True(t, sql.ErrSavepointDoesNotExist.Is(sess.RollbackToSavepoint(ctx, tx, "b")))

	require.NoError(t, sess.CreateSavepoint(ctx, tx, "a"))
	require.NoError(t, insertRows(ctx, table, personRow(4, "four", "four@b.c")))
	require.NoError(t, sess.CreateSavepoint(ctx, tx, "c"))
	require.NoError(t, sess.ReleaseSavepoint(ctx, tx, "a"))
	require.True(t, sql.ErrSavepointDoesNotExist.Is(sess.RollbackToSavepoint(ctx, tx, "c")))
	require.NoError(t, sess.CommitTransaction(ctx, tx))

	rows := readRows(t, base, table)
	require.Len(t, rows, 2)
	require.Equal(t, int64(1), rows[0][0])
	require.Equal(t, int64(4), rows[1][0])
}

func TestSavepointRollsBackOpenEditor(t *testing.T) {
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	base := sql.NewContext(context.Background())
	table := mustTable(t, base, createPeopleTable(t, base, store))
	sess, ctx := beginSession(t, store)
	tx := ctx.GetTransaction()

	require.NoError(t, insertRows(ctx, table, personRow(1, "one", "one@b.c")))
	require.NoError(t, sess.CreateSavepoint(ctx, tx, "s"))

	inserter := table.Inserter(ctx)
	inserter.StatementBegin(ctx)
	require.NoError(t, inserter.Insert(ctx, personRow(2, "two", "two@b.c")))
	require.NoError(t, sess.RollbackToSavepoint(ctx, tx, "s"))
	require.NoError(t, inserter.Close(ctx))
	require.NoError(t, sess.CommitTransaction(ctx, tx))

	rows := readRows(t, base, table)
	require.Len(t, rows, 1)
	require.Equal(t, int64(1), rows[0][0])
}

func TestLockingReadConflict(t *testing.T) {
	store := openAt(t, filepath.Join(t.TempDir(), "gms.db"))
	t.Cleanup(func() { _ = store.Close() })
	base := sql.NewContext(context.Background())
	table := mustTable(t, base, createPeopleTable(t, base, store))
	orig := personRow(1, "Jane", "jane@b.c")
	require.NoError(t, insertRows(base, table, orig))

	sess, ctx := beginSession(t, store)
	sess.SetLockingRead(true)
	require.Len(t, readRows(t, ctx, table), 1)
	sess.SetLockingRead(false)

	require.NoError(t, updateRow(base, table, orig, personRow(1, "Jane", "other@b.c")))
	err := sess.CommitTransaction(ctx, ctx.GetTransaction())
	require.Error(t, err)
	require.True(t, sql.ErrLockDeadlock.Is(err))
	require.Equal(t, "other@b.c", readRows(t, base, table)[0][2])
}

func beginSession(t *testing.T, store *Store) (*Session, *sql.Context) {
	t.Helper()
	sess := NewSession(sql.NewBaseSession(), store)
	ctx := sql.NewContext(context.Background(), sql.WithSession(sess))
	tx, err := sess.StartTransaction(ctx, sql.ReadWrite)
	require.NoError(t, err)
	ctx.SetTransaction(tx)
	ctx.SetIgnoreAutoCommit(true)
	return sess, ctx
}

func personRow(id int64, name, email string) sql.Row {
	created := time.Unix(0, 1667304000000001000).UTC()
	return sql.NewRow(id, name, email, types.MustJSON(`[]`), created)
}
