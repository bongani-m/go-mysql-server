package persist

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

type storedIndex struct {
	Name       string   `json:"name"`
	Columns    []string `json:"columns"`
	Lengths    []uint16 `json:"lengths,omitempty"`
	Descending []bool   `json:"descending,omitempty"`
	Constraint byte     `json:"constraint"`
	Comment    string   `json:"comment,omitempty"`
}

// Index is one secondary index. Lookups filter a full scan with the same range
// expression the in-memory tables use, so results stay correct without a
// separate index b-tree.
type Index struct {
	db         string
	table      string
	name       string
	columns    []string
	lengths    []uint16
	descending []bool
	schema     sql.Schema
	unique     bool
	spatial    bool
	full       bool
	vector     bool
	comment    string
}

func (idx *Index) ID() string                                 { return idx.name }
func (idx *Index) Database() string                           { return idx.db }
func (idx *Index) Table() string                              { return idx.table }
func (idx *Index) IsUnique() bool                             { return idx.unique }
func (idx *Index) IsSpatial() bool                            { return idx.spatial }
func (idx *Index) IsFullText() bool                           { return idx.full }
func (idx *Index) IsVector() bool                             { return idx.vector }
func (idx *Index) Comment() string                            { return idx.comment }
func (idx *Index) IndexType() string                          { return "BTREE" }
func (idx *Index) IsGenerated() bool                          { return false }
func (idx *Index) CanSupport(*sql.Context, ...sql.Range) bool { return true }
func (idx *Index) CanSupportOrderBy(sql.Expression) bool      { return false }

// Order reports no scan order. Rows are filtered from a full scan, so the
// engine must keep its own sort. ColumnOrders still records DESC for SHOW CREATE.
func (idx *Index) Order(*sql.Context) sql.IndexOrder { return sql.IndexOrderNone }

func (idx *Index) Reversible(*sql.Context) bool { return false }

func (idx *Index) ColumnOrders(*sql.Context) []sql.IndexColumnOrder {
	if len(idx.descending) == 0 {
		return nil
	}
	orders := make([]sql.IndexColumnOrder, len(idx.descending))
	any := false
	for i, down := range idx.descending {
		orders[i].Descending = down
		if down {
			any = true
		}
	}
	if !any {
		return nil
	}
	return orders
}

func (idx *Index) Expressions() []string {
	exprs := make([]string, len(idx.columns))
	for i, name := range idx.columns {
		exprs[i] = idx.table + "." + name
	}
	return exprs
}

func (idx *Index) PrefixLengths() []uint16 {
	for _, length := range idx.lengths {
		if length != 0 {
			return idx.lengths
		}
	}
	return nil
}

func (idx *Index) ColumnExpressionTypes(ctx *sql.Context) []sql.ColumnExpressionType {
	return expressionTypes(idx.Expressions(), idx.columnTypes(idx.columns))
}

func (idx *Index) CoversColumns(cols []string) bool {
	for _, col := range cols {
		found := false
		for _, name := range idx.columns {
			if strings.EqualFold(col, name) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func (idx *Index) ExtendedExpressions(ctx *sql.Context) []string {
	names := idx.extendedNames()
	exprs := make([]string, len(names))
	for i, name := range names {
		exprs[i] = idx.table + "." + name
	}
	return exprs
}

func (idx *Index) ExtendedColumnExpressionTypes(ctx *sql.Context) []sql.ColumnExpressionType {
	names := idx.extendedNames()
	return expressionTypes(idx.ExtendedExpressions(ctx), idx.columnTypes(names))
}

func (idx *Index) extendedNames() []string {
	names := append([]string(nil), idx.columns...)
	seen := make(map[string]struct{}, len(names))
	for _, name := range names {
		seen[strings.ToLower(name)] = struct{}{}
	}
	for _, col := range idx.schema {
		if !col.PrimaryKey {
			continue
		}
		if _, ok := seen[strings.ToLower(col.Name)]; ok {
			continue
		}
		names = append(names, col.Name)
	}
	return names
}

func (idx *Index) columnTypes(names []string) []sql.Type {
	types := make([]sql.Type, len(names))
	for i, name := range names {
		for _, col := range idx.schema {
			if strings.EqualFold(col.Name, name) {
				types[i] = col.Type
				break
			}
		}
	}
	return types
}

func (idx *Index) exprs() []sql.Expression {
	names := idx.extendedNames()
	exprs := make([]sql.Expression, len(names))
	for i, name := range names {
		for ord, col := range idx.schema {
			if !strings.EqualFold(col.Name, name) {
				continue
			}
			exprs[i] = expression.NewGetFieldWithTable(ord, 0, col.Type, idx.db, idx.table, col.Name, col.Nullable)
			break
		}
	}
	return exprs
}

func expressionTypes(exprs []string, colTypes []sql.Type) []sql.ColumnExpressionType {
	out := make([]sql.ColumnExpressionType, len(exprs))
	for i, expr := range exprs {
		out[i] = sql.ColumnExpressionType{Expression: expr, Type: colTypes[i]}
	}
	return out
}

func (t *Table) indexFromStored(stored storedIndex) *Index {
	return &Index{
		db:         t.dbName,
		table:      t.name,
		name:       stored.Name,
		columns:    append([]string(nil), stored.Columns...),
		lengths:    append([]uint16(nil), stored.Lengths...),
		descending: append([]bool(nil), stored.Descending...),
		schema:     t.meta.schema,
		unique:     sql.IndexConstraint(stored.Constraint) == sql.IndexConstraint_Unique,
		spatial:    sql.IndexConstraint(stored.Constraint) == sql.IndexConstraint_Spatial,
		full:       sql.IndexConstraint(stored.Constraint) == sql.IndexConstraint_Fulltext,
		vector:     sql.IndexConstraint(stored.Constraint) == sql.IndexConstraint_Vector,
		comment:    stored.Comment,
	}
}

func (t *Table) primaryIndex() *Index {
	if len(t.meta.pk) == 0 {
		return nil
	}
	columns := make([]string, len(t.meta.pk))
	for i, ord := range t.meta.pk {
		columns[i] = t.meta.schema[ord].Name
	}
	return &Index{
		db:      t.dbName,
		table:   t.name,
		name:    "PRIMARY",
		columns: columns,
		schema:  t.meta.schema,
		unique:  true,
	}
}

// GetIndexes implements sql.IndexAddressable.
func (t *Table) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	stored, err := t.readIndexes()
	if err != nil {
		return nil, err
	}
	// Secondary indexes are reported in name order, which is the order MySQL uses
	// for SHOW CREATE TABLE and information_schema.statistics. The primary key
	// always leads.
	sort.Slice(stored, func(i, j int) bool { return stored[i].Name < stored[j].Name })
	indexes := make([]sql.Index, 0, len(stored)+1)
	if primary := t.primaryIndex(); primary != nil {
		indexes = append(indexes, primary)
	}
	for _, idx := range stored {
		indexes = append(indexes, t.indexFromStored(idx))
	}
	return indexes, nil
}

// IndexedAccess implements sql.IndexAddressable.
func (t *Table) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	return &indexedTable{Table: t, lookup: lookup}
}

// PreciseMatch implements sql.IndexAddressable.
func (t *Table) PreciseMatch() bool { return true }

// CreateIndex implements sql.IndexAlterableTable.
func (t *Table) CreateIndex(ctx *sql.Context, indexDef sql.IndexDef) error {
	if indexDef.Name == "" {
		return fmt.Errorf("persist: index name is empty")
	}
	// An index over an expression is created in two steps: the engine first adds a
	// hidden generated column for the expression, then indexes it. This table
	// value predates that column, so its cached schema has to be reloaded.
	if err := t.refreshMeta(); err != nil {
		return err
	}
	indexes, err := t.readIndexes()
	if err != nil {
		return err
	}
	for _, idx := range indexes {
		if strings.EqualFold(idx.Name, indexDef.Name) {
			return sql.ErrDuplicateKey.New(indexDef.Name)
		}
	}
	stored := storedIndex{
		Name:       indexDef.Name,
		Columns:    make([]string, len(indexDef.Columns)),
		Lengths:    make([]uint16, len(indexDef.Columns)),
		Constraint: byte(indexDef.Constraint),
		Comment:    indexDef.Comment,
	}
	descending := make([]bool, len(indexDef.Columns))
	anyDescending := false
	for i, col := range indexDef.Columns {
		if col.Name == "" {
			return fmt.Errorf("persist: index %s column %d has no name", indexDef.Name, i)
		}
		ord := columnOrdinal(t.meta.schema, col.Name)
		if ord < 0 {
			return sql.ErrColumnNotFound.New(col.Name)
		}
		// Store the column's own spelling rather than the one the statement used:
		// index expressions are matched against "table.column" by exact string
		// comparison when the schema is displayed.
		stored.Columns[i] = t.meta.schema[ord].Name
		if col.Length > 0 && col.Length <= math.MaxUint16 {
			stored.Lengths[i] = uint16(col.Length)
		}
		if col.Order != nil && col.Order.Descending {
			descending[i] = true
			anyDescending = true
		}
	}
	if anyDescending {
		stored.Descending = descending
	}
	indexes = append(indexes, stored)
	return t.writeIndexes(indexes)
}

// DropIndex implements sql.IndexAlterableTable.
func (t *Table) DropIndex(ctx *sql.Context, indexName string) error {
	indexes, err := t.readIndexes()
	if err != nil {
		return err
	}
	next := indexes[:0]
	found := false
	for _, idx := range indexes {
		if strings.EqualFold(idx.Name, indexName) {
			found = true
			continue
		}
		next = append(next, idx)
	}
	if !found {
		return sql.ErrIndexNotFound.New(indexName)
	}
	return t.writeIndexes(next)
}

// RenameIndex implements sql.IndexAlterableTable.
func (t *Table) RenameIndex(ctx *sql.Context, fromIndexName, toIndexName string) error {
	if strings.EqualFold(fromIndexName, toIndexName) {
		return nil
	}
	indexes, err := t.readIndexes()
	if err != nil {
		return err
	}
	for _, idx := range indexes {
		if strings.EqualFold(idx.Name, toIndexName) {
			return sql.ErrDuplicateKey.New(toIndexName)
		}
	}
	found := false
	for i, idx := range indexes {
		if strings.EqualFold(idx.Name, fromIndexName) {
			indexes[i].Name = toIndexName
			found = true
			break
		}
	}
	if !found {
		return sql.ErrIndexNotFound.New(fromIndexName)
	}
	return t.writeIndexes(indexes)
}

func (t *Table) readIndexes() ([]storedIndex, error) {
	var raw []byte
	err := t.store.view(func(tx *kvTx) error {
		bucket := tableBucket(tx, t.dbName, t.name)
		if bucket == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		raw = append([]byte(nil), bucket.Get(keyIndexes)...)
		return nil
	})
	if err != nil || len(raw) == 0 {
		return nil, err
	}
	var indexes []storedIndex
	if err := json.Unmarshal(raw, &indexes); err != nil {
		return nil, err
	}
	return indexes, nil
}

func (t *Table) writeIndexes(indexes []storedIndex) error {
	raw, err := json.Marshal(indexes)
	if err != nil {
		return err
	}
	return t.store.update(func(tx *kvTx) error {
		bucket := tableBucket(tx, t.dbName, t.name)
		if bucket == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		return bucket.Put(keyIndexes, raw)
	})
}

type indexedTable struct {
	*Table
	lookup sql.IndexLookup
	// editor is set when the lookup comes from an open editor. Its buffered edits
	// have to be visible here, because a self-referential foreign key checks its
	// parent row through this path while the statement that wrote that row is
	// still running.
	editor *editor
}

func (t *indexedTable) LookupPartitions(ctx *sql.Context, lookup sql.IndexLookup) (sql.PartitionIter, error) {
	t.lookup = lookup
	return t.Partitions(ctx)
}

func (t *indexedTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	var buffered []edit
	if t.editor != nil {
		buffered = t.editor.edits
	}
	rows, err := t.Table.visibleRows(ctx, buffered...)
	if err != nil {
		return nil, err
	}
	iter := sql.RowIter(&rowIter{rows: rows})
	if t.lookup.IsEmptyRange {
		_ = iter.Close(ctx)
		return sql.RowsToRowIter(), nil
	}
	ranges, ok := t.lookup.Ranges.(sql.MySQLRangeCollection)
	if !ok {
		_ = iter.Close(ctx)
		return nil, fmt.Errorf("persist: index lookup ranges are %T", t.lookup.Ranges)
	}
	idx, ok := t.lookup.Index.(*Index)
	if !ok {
		return iter, nil
	}
	filter, err := expression.NewRangeFilterExpr(ctx, idx.exprs(), ranges)
	if err != nil {
		_ = iter.Close(ctx)
		return nil, err
	}
	if filter == nil {
		return iter, nil
	}
	return &filterIter{iter: iter, filter: filter}, nil
}

type filterIter struct {
	iter   sql.RowIter
	filter sql.Expression
}

func (it *filterIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		row, err := it.iter.Next(ctx)
		if err != nil {
			return nil, err
		}
		ok, err := it.filter.Eval(ctx, row)
		if err != nil {
			return nil, err
		}
		if sql.IsTrue(ok) {
			return row, nil
		}
	}
}

func (it *filterIter) Close(ctx *sql.Context) error {
	return it.iter.Close(ctx)
}

// GetNextAutoIncrementValue implements sql.AutoIncrementTable.
// Values outside the column type are ignored so a rejected insert does not
// move the sequence. The engine reports that rejection itself.
func (t *Table) GetNextAutoIncrementValue(ctx *sql.Context, insertVal interface{}) (uint64, error) {
	current, err := t.store.autoIncrement(t)
	if err != nil {
		return 0, err
	}
	if insertVal == nil {
		return current, nil
	}
	col := autoIncrementColumn(t.meta.schema)
	if col == nil {
		return current, nil
	}
	if _, inRange, convErr := col.Type.Convert(ctx, insertVal); convErr != nil || inRange != sql.InRange {
		return current, nil
	}
	cmp, err := types.Uint64.Compare(ctx, insertVal, current)
	if err != nil {
		return current, nil
	}
	if cmp > 0 {
		converted, _, err := types.Uint64.Convert(ctx, insertVal)
		if err != nil {
			return current, nil
		}
		current = converted.(uint64)
		if err := t.store.setAutoIncrement(t, current); err != nil {
			return 0, err
		}
	}
	return current, nil
}

// PeekNextAutoIncrementValue implements sql.AutoIncrementGetter.
func (t *Table) PeekNextAutoIncrementValue(ctx *sql.Context) (uint64, error) {
	current, err := t.store.autoIncrement(t)
	if err != nil {
		return 0, err
	}
	col := autoIncrementColumn(t.meta.schema)
	if col == nil {
		return current, nil
	}
	if _, inRange, convErr := col.Type.Convert(ctx, current); convErr == nil && inRange != sql.InRange && current > 0 {
		return current - 1, nil
	}
	return current, nil
}

func autoIncrementColumn(schema sql.Schema) *sql.Column {
	ord := autoIncrementOrdinal(schema)
	if ord < 0 {
		return nil
	}
	return schema[ord]
}

// AutoIncrementSetter implements sql.AutoIncrementTable.
func (t *Table) AutoIncrementSetter(ctx *sql.Context) sql.AutoIncrementSetter {
	return t.newEditor()
}

func (e *editor) SetAutoIncrementValue(ctx *sql.Context, val uint64) error {
	return e.table.store.setAutoIncrement(e.table, val)
}

func (e *editor) AcquireAutoIncrementLock(ctx *sql.Context) (func(), error) {
	return func() {}, nil
}

func (e *editor) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	return &indexedTable{Table: e.table, lookup: lookup, editor: e}
}

func (e *editor) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return e.table.GetIndexes(ctx)
}

func (e *editor) PreciseMatch() bool { return true }

func (s *Store) autoIncrement(t *Table) (uint64, error) {
	current := uint64(1)
	err := s.view(func(tx *kvTx) error {
		bucket := tableBucket(tx, t.dbName, t.name)
		if bucket == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		raw := bucket.Get(keyAutoInc)
		if len(raw) == 8 {
			current = binary.BigEndian.Uint64(raw)
		}
		return nil
	})
	return current, err
}

func (s *Store) setAutoIncrement(t *Table, val uint64) error {
	return s.update(func(tx *kvTx) error {
		bucket := tableBucket(tx, t.dbName, t.name)
		if bucket == nil {
			return sql.ErrTableNotFound.New(t.name)
		}
		var raw [8]byte
		binary.BigEndian.PutUint64(raw[:], val)
		return bucket.Put(keyAutoInc, raw[:])
	})
}

func (e *editor) noteAutoIncrement(ctx *sql.Context, row sql.Row) error {
	ord := autoIncrementOrdinal(e.meta.schema)
	if ord < 0 || ord >= len(row) || row[ord] == nil {
		return nil
	}
	current, err := e.table.store.autoIncrement(e.table)
	if err != nil {
		return err
	}
	col := e.meta.schema[ord]
	if _, inRange, convErr := col.Type.Convert(ctx, row[ord]); convErr != nil || inRange != sql.InRange {
		return nil
	}
	cmp, err := col.Type.Compare(ctx, row[ord], current)
	if err != nil {
		return err
	}
	if cmp > 0 {
		converted, _, err := types.Uint64.Convert(ctx, row[ord])
		if err != nil {
			return err
		}
		current = converted.(uint64)
	} else if cmp < 0 {
		return nil
	}
	bumpAutoIncrement(ctx, col, &current)
	return e.table.store.setAutoIncrement(e.table, current)
}

func bumpAutoIncrement(ctx *sql.Context, col *sql.Column, value *uint64) {
	if *value == math.MaxUint64 {
		return
	}
	next := *value + 1
	if _, inRange, err := col.Type.Convert(ctx, next); err == nil && inRange == sql.InRange {
		*value = next
	}
}

func autoIncrementOrdinal(schema sql.Schema) int {
	for i, col := range schema {
		if col.AutoIncrement {
			return i
		}
	}
	return -1
}

func (e *editor) checkUniqueIndexes(ctx *sql.Context, row sql.Row, skip sql.Row) error {
	indexes, err := e.table.readIndexes()
	if err != nil {
		return err
	}
	rows, err := e.matchingRows(ctx)
	if err != nil {
		return err
	}
	for _, idx := range indexes {
		if sql.IndexConstraint(idx.Constraint) != sql.IndexConstraint_Unique {
			continue
		}
		for _, existing := range rows {
			if skip != nil {
				same, err := rowsEqual(ctx, e.meta.schema, existing.row, skip)
				if err != nil {
					return err
				}
				if same {
					continue
				}
			}
			conflict, err := indexRowsConflict(ctx, e.meta.schema, idx, existing.row, row)
			if err != nil {
				return err
			}
			if conflict {
				return sql.NewUniqueKeyErr(idx.Name, false, existing.row)
			}
		}
	}
	return nil
}

func indexColumnsMatch(idx storedIndex, columns []string) bool {
	if len(idx.Columns) != len(columns) {
		return false
	}
	for i, name := range idx.Columns {
		if !strings.EqualFold(name, columns[i]) {
			return false
		}
	}
	return true
}

func indexRowsConflict(ctx *sql.Context, schema sql.Schema, idx storedIndex, existing, row sql.Row) (bool, error) {
	for i, name := range idx.Columns {
		ord := columnOrdinal(schema, name)
		if ord < 0 || ord >= len(existing) || ord >= len(row) {
			return false, fmt.Errorf("persist: index %s column %s is not in the schema", idx.Name, name)
		}
		if existing[ord] == nil || row[ord] == nil {
			return false, nil
		}
		left := existing[ord]
		right := row[ord]
		if i < len(idx.Lengths) && idx.Lengths[i] > 0 {
			left = prefixValue(left, idx.Lengths[i])
			right = prefixValue(right, idx.Lengths[i])
		}
		cmp, err := schema[ord].Type.Compare(ctx, left, right)
		if err != nil {
			return false, err
		}
		if cmp != 0 {
			return false, nil
		}
	}
	return true, nil
}

func prefixValue(value interface{}, length uint16) interface{} {
	switch v := value.(type) {
	case string:
		if len(v) > int(length) {
			return v[:length]
		}
	case []byte:
		if len(v) > int(length) {
			return v[:length]
		}
	}
	return value
}

func columnOrdinal(schema sql.Schema, name string) int {
	for i, col := range schema {
		if strings.EqualFold(col.Name, name) {
			return i
		}
	}
	return -1
}

// CreateIndexForForeignKey implements sql.ForeignKeyTable.
func (t *Table) CreateIndexForForeignKey(ctx *sql.Context, indexDef sql.IndexDef) error {
	return t.CreateIndex(ctx, indexDef)
}

// GetDeclaredForeignKeys implements sql.ForeignKeyTable.
func (t *Table) GetDeclaredForeignKeys(ctx *sql.Context) ([]sql.ForeignKeyConstraint, error) {
	keys, err := t.database().readForeignKeys()
	if err != nil {
		return nil, err
	}
	var declared []sql.ForeignKeyConstraint
	for _, key := range keys {
		if strings.EqualFold(key.Table, t.name) {
			declared = append(declared, key)
		}
	}
	sort.Slice(declared, func(i, j int) bool { return declared[i].Name < declared[j].Name })
	return declared, nil
}

// GetReferencedForeignKeys implements sql.ForeignKeyTable.
func (t *Table) GetReferencedForeignKeys(ctx *sql.Context) ([]sql.ForeignKeyConstraint, error) {
	keys, err := t.database().readForeignKeys()
	if err != nil {
		return nil, err
	}
	var referenced []sql.ForeignKeyConstraint
	for _, key := range keys {
		if strings.EqualFold(key.ParentTable, t.name) {
			referenced = append(referenced, key)
		}
	}
	sort.Slice(referenced, func(i, j int) bool { return referenced[i].Name < referenced[j].Name })
	return referenced, nil
}

// AddForeignKey implements sql.ForeignKeyTable.
func (t *Table) AddForeignKey(ctx *sql.Context, fk sql.ForeignKeyConstraint) error {
	db := t.database()
	keys, err := db.readForeignKeys()
	if err != nil {
		return err
	}
	for _, key := range keys {
		if strings.EqualFold(key.Name, fk.Name) {
			return sql.ErrForeignKeyDuplicateName.New(fk.Name)
		}
	}
	keys = append(keys, fk)
	return db.writeForeignKeys(keys)
}

// DropForeignKey implements sql.ForeignKeyTable.
func (t *Table) DropForeignKey(ctx *sql.Context, fkName string, tableName string, schemaName string) error {
	db := t.database()
	keys, err := db.readForeignKeys()
	if err != nil {
		return err
	}
	next := keys[:0]
	found := false
	for _, key := range keys {
		if strings.EqualFold(key.Name, fkName) && (tableName == "" || strings.EqualFold(key.Table, tableName)) {
			found = true
			continue
		}
		next = append(next, key)
	}
	if !found {
		return sql.ErrForeignKeyNotFound.New(fkName, t.name)
	}
	return db.writeForeignKeys(next)
}

// UpdateForeignKey implements sql.ForeignKeyTable.
func (t *Table) UpdateForeignKey(ctx *sql.Context, fkName string, fk sql.ForeignKeyConstraint) error {
	db := t.database()
	keys, err := db.readForeignKeys()
	if err != nil {
		return err
	}
	found := false
	for i, key := range keys {
		if strings.EqualFold(key.Name, fkName) {
			keys[i] = fk
			found = true
			break
		}
	}
	if !found {
		return sql.ErrForeignKeyNotFound.New(fkName, t.name)
	}
	return db.writeForeignKeys(keys)
}

// GetForeignKeyEditor implements sql.ForeignKeyTable.
func (t *Table) GetForeignKeyEditor(ctx *sql.Context) sql.ForeignKeyEditor {
	return t.newEditor()
}

func (t *Table) database() *Database {
	return &Database{store: t.store, name: t.dbName}
}

// RowCount implements sql.StatisticsTable. The count includes uncommitted edits.
func (t *Table) RowCount(ctx *sql.Context) (uint64, bool, error) {
	rows, err := t.visibleRows(ctx)
	if err != nil {
		return 0, false, err
	}
	return uint64(len(rows)), true, nil
}

// DataLength implements sql.StatisticsTable.
func (t *Table) DataLength(ctx *sql.Context) (uint64, error) {
	rows, err := t.visibleRows(ctx)
	if err != nil {
		return 0, err
	}
	var n uint64
	for _, row := range rows {
		raw, err := encodeRow(ctx, row)
		if err != nil {
			return 0, err
		}
		n += uint64(len(raw))
	}
	return n, nil
}
