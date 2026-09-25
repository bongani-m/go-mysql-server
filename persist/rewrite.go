package persist

import (
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var _ sql.RewritableTable = (*Table)(nil)

// ShouldRewriteTable implements sql.RewritableTable.
//
// Every schema change is served by a rewrite. The alternative path calls
// AddColumn / DropColumn / ModifyColumn and then backfills through an ordinary
// UPDATE, which leaves the GetField indexes inside column default expressions
// pointing at the pre-alter column positions. The rewrite path hands us rows
// that the engine has already projected into the new schema, so ADD COLUMN ...
// FIRST with a default that references another column lands on the right value.
func (t *Table) ShouldRewriteTable(ctx *sql.Context, oldSchema, newSchema sql.PrimaryKeySchema, oldColumn, newColumn *sql.Column) bool {
	return true
}

// RewriteInserter implements sql.RewritableTable.
func (t *Table) RewriteInserter(
	ctx *sql.Context,
	oldSchema, newSchema sql.PrimaryKeySchema,
	oldColumn, newColumn *sql.Column,
	idxCols []sql.IndexColumn,
) (sql.RowInserter, error) {
	if len(oldSchema.PkOrdinals) > 0 && len(newSchema.PkOrdinals) == 0 {
		if err := sql.ValidatePrimaryKeyDrop(ctx, t, oldSchema); err != nil {
			return nil, err
		}
	}
	if len(oldSchema.PkOrdinals) != len(newSchema.PkOrdinals) {
		for _, idxCol := range idxCols {
			ord := columnOrdinal(newSchema.Schema, idxCol.Name)
			if ord < 0 {
				return nil, sql.ErrColumnNotFound.New(idxCol.Name)
			}
			col := newSchema.Schema[ord]
			if col.PrimaryKey && idxCol.Length > 0 && types.IsText(col.Type) {
				return nil, sql.ErrUnsupportedIndexPrefix.New(col.Name)
			}
		}
	}
	// A primary key column is never nullable, but MODIFY COLUMN does not restate
	// the key, so the new column description arrives with the DDL's (absent)
	// nullability rather than the one the key implies.
	newSchema.Schema = newSchema.Schema.Copy()
	for _, ord := range newSchema.PkOrdinals {
		if ord >= 0 && ord < len(newSchema.Schema) {
			newSchema.Schema[ord].PrimaryKey = true
			newSchema.Schema[ord].Nullable = false
		}
	}
	return &rewriter{table: t, schema: newSchema, oldColumn: oldColumn, newColumn: newColumn}, nil
}

// rewriter buffers the rows of a table rewrite. The table has to keep answering
// scans in the old schema while the rewrite runs, so nothing is written until
// Close.
type rewriter struct {
	table     *Table
	schema    sql.PrimaryKeySchema
	oldColumn *sql.Column
	newColumn *sql.Column
	rows      []sql.Row
	discarded bool
	closed    bool
}

var _ sql.RowInserter = (*rewriter)(nil)

func (r *rewriter) StatementBegin(*sql.Context) {}

func (r *rewriter) DiscardChanges(_ *sql.Context, _ error) error {
	r.discarded = true
	r.rows = nil
	return nil
}

func (r *rewriter) StatementComplete(*sql.Context) error { return nil }

func (r *rewriter) Insert(ctx *sql.Context, row sql.Row) error {
	if len(row) != len(r.schema.Schema) {
		return sql.ErrUnexpectedRowLength.New(len(r.schema.Schema), len(row))
	}
	r.rows = append(r.rows, row.Copy())
	return nil
}

func (r *rewriter) Close(ctx *sql.Context) error {
	if r.closed || r.discarded {
		return nil
	}
	r.closed = true
	if err := r.table.writeSchema(ctx, r.schema.Schema, r.schema.PkOrdinals, r.rows); err != nil {
		return err
	}
	return r.table.reconcileIndexes(r.oldColumn, r.newColumn)
}

// reconcileIndexes brings the stored index definitions back in line with the
// schema the rewrite just installed: a renamed column is renamed inside the
// definitions, and an index over a column that no longer exists is dropped.
func (t *Table) reconcileIndexes(oldColumn, newColumn *sql.Column) error {
	indexes, err := t.readIndexes()
	if err != nil || len(indexes) == 0 {
		return err
	}
	changed := false
	kept := make([]storedIndex, 0, len(indexes))
	for _, idx := range indexes {
		drop := false
		for i, name := range idx.Columns {
			if oldColumn != nil && newColumn != nil && strings.EqualFold(name, oldColumn.Name) {
				name = newColumn.Name
			}
			ord := columnOrdinal(t.meta.schema, name)
			if ord < 0 {
				drop = true
				break
			}
			if idx.Columns[i] != t.meta.schema[ord].Name {
				idx.Columns[i] = t.meta.schema[ord].Name
				changed = true
			}
		}
		if drop {
			changed = true
			continue
		}
		kept = append(kept, idx)
	}
	if !changed {
		return nil
	}
	return t.writeIndexes(kept)
}
