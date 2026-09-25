package persist

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/dgraph-io/badger/v4"

	"github.com/dolthub/go-mysql-server/sql"
)

var (
	bucketDatabases = []byte("databases")
	bucketTables    = []byte("tables")
	bucketRows      = []byte("rows")
	keyName         = []byte("name")
	keySchema       = []byte("schema")
	keyComment      = []byte("comment")
	keyCollation    = []byte("collation")
	keyViews        = []byte("views")
	keyIndexes      = []byte("indexes")
	keyForeignKeys  = []byte("foreignKeys")
	keyTriggers     = []byte("triggers")
	keyProcedures   = []byte("procedures")
	keyEvents       = []byte("events")
	keyAutoInc      = []byte("autoinc")
	keyChecks       = []byte("checks")
	keyTargetRows   = []byte("targetRowSize")
)

// Store is a go-mysql-server database provider backed by one Badger directory.
type Store struct {
	db   *badger.DB
	path string
	mu   sync.Mutex
}

var _ sql.DatabaseProvider = (*Store)(nil)
var _ sql.MutableDatabaseProvider = (*Store)(nil)

// Open opens or creates the Badger directory at path.
// GMS_DATA is the usual way to choose the path; the server default is data/gms.
func Open(path string) (*Store, error) {
	db, err := openBadger(path)
	if err != nil {
		return nil, err
	}
	return &Store{db: db, path: path}, nil
}

// Close releases the file lock.
func (s *Store) Close() error {
	return s.db.Close()
}

// Path returns the Badger directory path.
func (s *Store) Path() string {
	return s.path
}

// Database implements sql.DatabaseProvider.
func (s *Store) Database(ctx *sql.Context, name string) (sql.Database, error) {
	db, ok, err := s.loadDatabase(name)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, sql.ErrDatabaseNotFound.New(name)
	}
	return db, nil
}

// HasDatabase implements sql.DatabaseProvider.
func (s *Store) HasDatabase(ctx *sql.Context, name string) bool {
	_, ok, err := s.loadDatabase(name)
	return err == nil && ok
}

// AllDatabases implements sql.DatabaseProvider.
func (s *Store) AllDatabases(ctx *sql.Context) []sql.Database {
	var names []string
	_ = s.view(func(tx *kvTx) error {
		root := tx.Bucket(bucketDatabases)
		if root == nil {
			return nil
		}
		return root.ForEach(func(k, v []byte) error {
			if v != nil {
				return nil
			}
			bucket := root.Bucket(k)
			if bucket == nil {
				return nil
			}
			names = append(names, string(bucket.Get(keyName)))
			return nil
		})
	})
	sort.Strings(names)
	dbs := make([]sql.Database, len(names))
	for i, name := range names {
		dbs[i] = &Database{store: s, name: name}
	}
	return dbs
}

// CreateDatabase implements sql.MutableDatabaseProvider.
func (s *Store) CreateDatabase(ctx *sql.Context, name string) error {
	if name == "" {
		return fmt.Errorf("persist: database name is empty")
	}
	return s.update(func(tx *kvTx) error {
		root, err := tx.CreateBucketIfNotExists(bucketDatabases)
		if err != nil {
			return err
		}
		key := bucketKey(name)
		if root.Bucket(key) != nil {
			return sql.ErrDatabaseExists.New(name)
		}
		bucket, err := root.CreateBucket(key)
		if err != nil {
			return err
		}
		if err := bucket.Put(keyName, []byte(name)); err != nil {
			return err
		}
		var coll [2]byte
		binary.BigEndian.PutUint16(coll[:], uint16(sql.Collation_Default))
		if err := bucket.Put(keyCollation, coll[:]); err != nil {
			return err
		}
		_, err = bucket.CreateBucket(bucketTables)
		return err
	})
}

// DropDatabase implements sql.MutableDatabaseProvider.
func (s *Store) DropDatabase(ctx *sql.Context, name string) error {
	return s.update(func(tx *kvTx) error {
		root := tx.Bucket(bucketDatabases)
		if root == nil || root.Bucket(bucketKey(name)) == nil {
			return sql.ErrDatabaseNotFound.New(name)
		}
		return root.DeleteBucket(bucketKey(name))
	})
}

func (s *Store) loadDatabase(name string) (*Database, bool, error) {
	var stored string
	var found bool
	err := s.view(func(tx *kvTx) error {
		bucket := databaseBucket(tx, name)
		if bucket == nil {
			return nil
		}
		found = true
		stored = string(bucket.Get(keyName))
		return nil
	})
	if err != nil || !found {
		return nil, false, err
	}
	return &Database{store: s, name: stored}, true, nil
}

// Database is one persisted MySQL database.
type Database struct {
	store *Store
	name  string
}

var _ sql.Database = (*Database)(nil)
var _ sql.TableCreator = (*Database)(nil)
var _ sql.TableDropper = (*Database)(nil)

// Name implements sql.Nameable.
func (d *Database) Name() string { return d.name }

// GetTableInsensitive implements sql.Database.
func (d *Database) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	meta, name, ok, err := d.store.loadTable(d.name, tblName)
	if err != nil || !ok {
		return nil, false, err
	}
	return &Table{store: d.store, dbName: d.name, name: name, meta: meta}, true, nil
}

// GetTableNames implements sql.Database.
func (d *Database) GetTableNames(ctx *sql.Context) ([]string, error) {
	var names []string
	err := d.store.view(func(tx *kvTx) error {
		tables := tablesBucket(tx, d.name)
		if tables == nil {
			return sql.ErrDatabaseNotFound.New(d.name)
		}
		return tables.ForEach(func(k, v []byte) error {
			if v != nil {
				return nil
			}
			bucket := tables.Bucket(k)
			if bucket == nil {
				return nil
			}
			names = append(names, string(bucket.Get(keyName)))
			return nil
		})
	})
	sort.Strings(names)
	return names, err
}

// CreateTable implements sql.TableCreator.
func (d *Database) CreateTable(ctx *sql.Context, name string, schema sql.PrimaryKeySchema, collation sql.CollationID, comment string) error {
	if name == "" {
		return fmt.Errorf("persist: table name is empty")
	}
	if collation == sql.Collation_Unspecified {
		collation = sql.Collation_Default
	}
	raw, err := encodeSchema(ctx, schema, collation, comment)
	if err != nil {
		return err
	}
	return d.store.update(func(tx *kvTx) error {
		tables := tablesBucket(tx, d.name)
		if tables == nil {
			return sql.ErrDatabaseNotFound.New(d.name)
		}
		if tables.Bucket(bucketKey(name)) != nil {
			return sql.ErrTableAlreadyExists.New(name)
		}
		bucket, err := tables.CreateBucket(bucketKey(name))
		if err != nil {
			return err
		}
		if err := bucket.Put(keyName, []byte(name)); err != nil {
			return err
		}
		if err := bucket.Put(keySchema, raw); err != nil {
			return err
		}
		if comment != "" {
			if err := bucket.Put(keyComment, []byte(comment)); err != nil {
				return err
			}
		}
		var coll [2]byte
		binary.BigEndian.PutUint16(coll[:], uint16(collation))
		if err := bucket.Put(keyCollation, coll[:]); err != nil {
			return err
		}
		_, err = bucket.CreateBucket(bucketRows)
		return err
	})
}

// RenameTable implements sql.TableRenamer.
func (d *Database) RenameTable(ctx *sql.Context, oldName, newName string) error {
	if newName == "" {
		return fmt.Errorf("persist: table name is empty")
	}
	err := d.store.update(func(tx *kvTx) error {
		tables := tablesBucket(tx, d.name)
		if tables == nil {
			return sql.ErrDatabaseNotFound.New(d.name)
		}
		src := tables.Bucket(bucketKey(oldName))
		if src == nil {
			return sql.ErrTableNotFound.New(oldName)
		}
		if strings.EqualFold(oldName, newName) {
			return src.Put(keyName, []byte(newName))
		}
		if tables.Bucket(bucketKey(newName)) != nil {
			return sql.ErrTableAlreadyExists.New(newName)
		}
		dst, err := tables.CreateBucket(bucketKey(newName))
		if err != nil {
			return err
		}
		if err := copyBucket(src, dst); err != nil {
			return err
		}
		if err := dst.SetSequence(src.Sequence()); err != nil {
			return err
		}
		if err := dst.Put(keyName, []byte(newName)); err != nil {
			return err
		}
		return tables.DeleteBucket(bucketKey(oldName))
	})
	if err != nil {
		return err
	}
	if sess, ok := sessionFrom(ctx); ok {
		sess.rename(tableRef{db: strings.ToLower(d.name), name: strings.ToLower(oldName)}, tableRef{db: strings.ToLower(d.name), name: strings.ToLower(newName)})
	}
	return nil
}

func copyBucket(src, dst *kvBucket) error {
	return src.ForEach(func(k, v []byte) error {
		key := append([]byte(nil), k...)
		if v != nil {
			return dst.Put(key, append([]byte(nil), v...))
		}
		nested := src.Bucket(k)
		child, err := dst.CreateBucket(key)
		if err != nil {
			return err
		}
		if err := child.SetSequence(nested.Sequence()); err != nil {
			return err
		}
		return copyBucket(nested, child)
	})
}

// DropTable implements sql.TableDropper.
func (d *Database) DropTable(ctx *sql.Context, name string) error {
	return d.store.update(func(tx *kvTx) error {
		tables := tablesBucket(tx, d.name)
		if tables == nil {
			return sql.ErrDatabaseNotFound.New(d.name)
		}
		if tables.Bucket(bucketKey(name)) == nil {
			return sql.ErrTableNotFound.New(name)
		}
		return tables.DeleteBucket(bucketKey(name))
	})
}

func (s *Store) loadTable(dbName, tableName string) (tableMeta, string, bool, error) {
	var raw []byte
	var name string
	var targetRowSize uint64
	err := s.view(func(tx *kvTx) error {
		tables := tablesBucket(tx, dbName)
		if tables == nil {
			return sql.ErrDatabaseNotFound.New(dbName)
		}
		bucket := tables.Bucket(bucketKey(tableName))
		if bucket == nil {
			return nil
		}
		raw = append([]byte(nil), bucket.Get(keySchema)...)
		name = string(bucket.Get(keyName))
		if size := bucket.Get(keyTargetRows); len(size) == 8 {
			targetRowSize = binary.BigEndian.Uint64(size)
		}
		return nil
	})
	if err != nil || raw == nil {
		return tableMeta{}, "", false, err
	}
	meta, err := decodeSchema(raw, dbName, name)
	if err != nil {
		return tableMeta{}, "", false, err
	}
	meta.targetRowSize = targetRowSize
	return meta, name, true, nil
}

type storedRow struct {
	key []byte
	row sql.Row
}

func (s *Store) diskRows(ctx context.Context, t *Table) ([]storedRow, error) {
	var raws []struct {
		key []byte
		val []byte
	}
	err := s.view(func(tx *kvTx) error {
		rows := rowsBucket(tx, t.dbName, t.name)
		if rows == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		return rows.ForEach(func(k, v []byte) error {
			raws = append(raws, struct {
				key []byte
				val []byte
			}{key: append([]byte(nil), k...), val: append([]byte(nil), v...)})
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	out := make([]storedRow, len(raws))
	for i, raw := range raws {
		row, err := decodeRow(ctx, t.meta.schema, raw.val)
		if err != nil {
			return nil, err
		}
		out[i] = storedRow{key: raw.key, row: row}
	}
	return out, nil
}

func (s *Store) getRow(ctx context.Context, t *Table, key []byte) (sql.Row, bool, error) {
	var raw []byte
	err := s.view(func(tx *kvTx) error {
		rows := rowsBucket(tx, t.dbName, t.name)
		if rows == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		raw = append([]byte(nil), rows.Get(key)...)
		return nil
	})
	if err != nil || len(raw) == 0 {
		return nil, false, err
	}
	row, err := decodeRow(ctx, t.meta.schema, raw)
	return row, err == nil, err
}

func (s *Store) nextSequence(t *Table) ([]byte, error) {
	var key []byte
	err := s.update(func(tx *kvTx) error {
		rows := rowsBucket(tx, t.dbName, t.name)
		if rows == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		seq, err := rows.NextSequence()
		if err != nil {
			return err
		}
		key = sequenceKey(seq)
		return nil
	})
	return key, err
}

func (s *Store) apply(t *Table, edits []edit) error {
	return s.applyAll(map[tableRef][]edit{t.ref(): edits})
}

func (s *Store) applyAll(pending map[tableRef][]edit) error {
	if len(pending) == 0 {
		return nil
	}
	return s.update(func(tx *kvTx) error {
		for ref, edits := range pending {
			if err := applyEdits(tx, ref, edits); err != nil {
				return err
			}
		}
		return nil
	})
}

func applyEdits(tx *kvTx, ref tableRef, edits []edit) error {
	rows := rowsBucket(tx, ref.db, ref.name)
	if rows == nil {
		return sql.ErrTableNotFound.New(ref.name)
	}
	for _, ed := range edits {
		switch ed.op {
		case opDelete:
			if err := rows.Delete(ed.key); err != nil {
				return err
			}
		case opUpdate:
			if len(ed.oldKey) > 0 && !bytesEqual(ed.oldKey, ed.key) {
				if err := rows.Delete(ed.oldKey); err != nil {
					return err
				}
			}
			if existing := rows.Get(ed.key); existing != nil && !bytesEqual(ed.oldKey, ed.key) {
				return sql.ErrPrimaryKeyViolation.New()
			}
			if err := rows.Put(ed.key, ed.raw); err != nil {
				return err
			}
		case opInsert:
			if rows.Get(ed.key) != nil {
				return sql.ErrPrimaryKeyViolation.New()
			}
			if err := rows.Put(ed.key, ed.raw); err != nil {
				return err
			}
		default:
			return fmt.Errorf("persist: unknown edit %d", ed.op)
		}
	}
	return nil
}

func (s *Store) truncate(t *Table) (int, error) {
	var n int
	err := s.update(func(tx *kvTx) error {
		bucket := tableBucket(tx, t.dbName, t.name)
		if bucket == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		rows := bucket.Bucket(bucketRows)
		if rows == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		if err := rows.ForEach(func(_, _ []byte) error {
			n++
			return nil
		}); err != nil {
			return err
		}
		if err := bucket.DeleteBucket(bucketRows); err != nil {
			return err
		}
		_, err := bucket.CreateBucket(bucketRows)
		return err
	})
	return n, err
}

func bucketKey(name string) []byte {
	return []byte(strings.ToLower(name))
}

func databaseBucket(tx *kvTx, name string) *kvBucket {
	root := tx.Bucket(bucketDatabases)
	if root == nil {
		return nil
	}
	return root.Bucket(bucketKey(name))
}

func tablesBucket(tx *kvTx, dbName string) *kvBucket {
	db := databaseBucket(tx, dbName)
	if db == nil {
		return nil
	}
	return db.Bucket(bucketTables)
}

func tableBucket(tx *kvTx, dbName, tableName string) *kvBucket {
	tables := tablesBucket(tx, dbName)
	if tables == nil {
		return nil
	}
	return tables.Bucket(bucketKey(tableName))
}

func rowsBucket(tx *kvTx, dbName, tableName string) *kvBucket {
	table := tableBucket(tx, dbName, tableName)
	if table == nil {
		return nil
	}
	return table.Bucket(bucketRows)
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
