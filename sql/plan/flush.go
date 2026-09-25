// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// FlushPrivileges reads privileges from mysql tables and registers any unregistered privileges found.
type FlushPrivileges struct {
	MysqlDb        sql.Database
	writesToBinlog bool
}

var _ sql.Node = (*FlushPrivileges)(nil)
var _ sql.CollationCoercible = (*FlushPrivileges)(nil)
var _ sql.Databaser = (*FlushPrivileges)(nil)

// NewFlushPrivileges creates a new FlushPrivileges node.
func NewFlushPrivileges(ft bool) *FlushPrivileges {
	return &FlushPrivileges{
		writesToBinlog: ft,
		MysqlDb:        sql.UnresolvedDatabase("mysql"),
	}
}

// String implements the interface sql.Node.
func (*FlushPrivileges) String() string { return "FLUSH PRIVILEGES" }

// WithChildren implements the interface sql.Node.
func (f *FlushPrivileges) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 0)
	}

	return f, nil
}

// CollationCoercibility implements the interface sql.CollationCoercible.
func (*FlushPrivileges) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

// Semantically there is no reason to run this in a read-only context, so we say it is not read only.
func (*FlushPrivileges) IsReadOnly() bool {
	return false
}

// Resolved implements the interface sql.Node.
func (f *FlushPrivileges) Resolved() bool {
	_, ok := f.MysqlDb.(sql.UnresolvedDatabase)
	return !ok
}

// Children implements the sql.Node interface.
func (*FlushPrivileges) Children() []sql.Node { return nil }

// Schema implements the sql.Node interface.
func (*FlushPrivileges) Schema(ctx *sql.Context) sql.Schema { return types.OkResultSchema }

// Database implements the sql.Databaser interface.
func (f *FlushPrivileges) Database() sql.Database {
	return f.MysqlDb
}

// WithDatabase implements the sql.Databaser interface.
func (f *FlushPrivileges) WithDatabase(db sql.Database) (sql.Node, error) {
	fp := *f
	fp.MysqlDb = db
	return &fp, nil
}

// BinaryLogRotator rolls the binary log. Integrators that do not implement
// it keep FLUSH BINARY LOGS as a no-op.
type BinaryLogRotator interface {
	RotateBinaryLog(ctx *sql.Context) error
}

// FlushBinaryLogs is FLUSH BINARY LOGS for a rotator.
type FlushBinaryLogs struct {
	Rotator BinaryLogRotator
}

var _ sql.Node = (*FlushBinaryLogs)(nil)
var _ sql.ExecSourceRel = (*FlushBinaryLogs)(nil)
var _ sql.CollationCoercible = (*FlushBinaryLogs)(nil)

// NewFlushBinaryLogs returns a FLUSH BINARY LOGS node.
func NewFlushBinaryLogs(rotator BinaryLogRotator) *FlushBinaryLogs {
	return &FlushBinaryLogs{Rotator: rotator}
}

func (*FlushBinaryLogs) String() string { return "FLUSH BINARY LOGS" }

func (*FlushBinaryLogs) Resolved() bool { return true }

func (*FlushBinaryLogs) IsReadOnly() bool { return false }

func (*FlushBinaryLogs) Schema(*sql.Context) sql.Schema { return nil }

func (*FlushBinaryLogs) Children() []sql.Node { return nil }

func (f *FlushBinaryLogs) WithChildren(ctx *sql.Context, children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(f, len(children), 0)
	}
	return f, nil
}

func (*FlushBinaryLogs) CollationCoercibility(ctx *sql.Context) (collation sql.CollationID, coercibility byte) {
	return sql.Collation_binary, 7
}

// RowIter rolls the binary log.
func (f *FlushBinaryLogs) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	if f.Rotator != nil {
		if err := f.Rotator.RotateBinaryLog(ctx); err != nil {
			return nil, err
		}
	}
	return sql.RowsToRowIter(), nil
}
