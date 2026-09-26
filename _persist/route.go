package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/persist"
)

// RouteKind is where one statement runs.
type RouteKind int

const (
	// RouteLocal runs on this node's shard, including statements that are not
	// table data (BEGIN, SET, SHOW).
	RouteLocal RouteKind = iota
	// RouteShard runs on one shard.
	RouteShard
	// RouteScatter reads every shard and merges the rows.
	RouteScatter
	// RouteDDL is schema. It is applied to meta and to every shard.
	RouteDDL
	// RoutePlace records a table placement in meta.
	RoutePlace
)

// Route is the decision for one statement.
type Route struct {
	Kind   RouteKind
	Shard  int
	Place  persist.TablePlacement
	Checks []routedValue
}

// routedValue is one unique-column value that must be absent on every shard.
type routedValue struct {
	Column string
	Text   string
	Quote  bool
}

// RouteQuery decides which shard owns query. pinned is the shard an open
// transaction already used, or -1. n is the number of shards.
func RouteQuery(query, db string, binds []persist.ForwardBind, places []persist.TablePlacement, n, pinned int) (Route, error) {
	if p, ok := parseShardStmt(query); ok {
		return Route{Kind: RoutePlace, Place: p}, nil
	}
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		if pinned >= 0 {
			return Route{}, fmt.Errorf("persist: cannot route %q", query)
		}
		return Route{Kind: RouteLocal}, nil
	}
	switch stmt.(type) {
	case *sqlparser.DDL, *sqlparser.DBDDL:
		return Route{Kind: RouteDDL}, nil
	case *sqlparser.Begin, *sqlparser.Commit, *sqlparser.Rollback, *sqlparser.Set, *sqlparser.Use, *sqlparser.Show, *sqlparser.Explain, *sqlparser.OtherAdmin:
		return Route{Kind: RouteLocal}, nil
	}
	ids, scatter, place, checks, err := shardIDs(stmt, db, binds, places)
	if err != nil {
		return Route{}, err
	}
	if place.Table == "" {
		return Route{Kind: RouteLocal}, nil
	}
	if scatter {
		if !isReadStmt(stmt) {
			return Route{}, fmt.Errorf("persist: statement spans shards")
		}
		return Route{Kind: RouteScatter, Place: place}, nil
	}
	if len(ids) == 0 {
		if isReadStmt(stmt) {
			return Route{Kind: RouteScatter, Place: place}, nil
		}
		return Route{}, fmt.Errorf("persist: statement has no shard key")
	}
	shard := persist.ShardIndex(ids[0], n)
	for _, id := range ids[1:] {
		if persist.ShardIndex(id, n) != shard {
			return Route{}, fmt.Errorf("persist: statement spans shards")
		}
	}
	if pinned >= 0 && pinned != shard {
		return Route{}, fmt.Errorf("persist: transaction spans shards")
	}
	return Route{Kind: RouteShard, Shard: shard, Place: place, Checks: checks}, nil
}

func isReadStmt(stmt sqlparser.Statement) bool {
	sel, ok := stmt.(*sqlparser.Select)
	if !ok {
		return false
	}
	return !hasLock(sel.Lock) && sel.Into == nil
}

func shardIDs(stmt sqlparser.Statement, db string, binds []persist.ForwardBind, places []persist.TablePlacement) ([]int64, bool, persist.TablePlacement, []routedValue, error) {
	switch n := stmt.(type) {
	case *sqlparser.Select:
		name, ok := singleTable(n.From)
		if !ok {
			return nil, true, persist.TablePlacement{}, nil, nil
		}
		place, ok := findPlace(places, db, name)
		if !ok {
			return nil, false, persist.TablePlacement{}, nil, nil
		}
		if n.Where == nil {
			return nil, true, place, nil, nil
		}
		expr, ok := equality(n.Where.Expr, place.Column)
		if !ok {
			return nil, true, place, nil, nil
		}
		id, ok, err := intValue(expr, binds)
		if err != nil || !ok {
			return nil, true, place, nil, err
		}
		return []int64{id}, false, place, nil, nil
	case *sqlparser.Insert:
		place, ok := findPlace(places, db, n.Table)
		if !ok {
			return nil, false, persist.TablePlacement{}, nil, nil
		}
		ids, err := insertIDs(n, place.Column, binds)
		if err != nil {
			return nil, false, place, nil, err
		}
		checks, err := insertChecks(n, place, binds)
		if err != nil {
			return nil, false, place, nil, err
		}
		return ids, false, place, checks, nil
	case *sqlparser.Update:
		name, ok := singleTable(n.TableExprs)
		if !ok {
			return nil, false, persist.TablePlacement{}, nil, fmt.Errorf("persist: statement spans shards")
		}
		place, ok := findPlace(places, db, name)
		if !ok {
			return nil, false, persist.TablePlacement{}, nil, nil
		}
		if n.Where == nil {
			return nil, false, place, nil, fmt.Errorf("persist: statement spans shards")
		}
		expr, ok := equality(n.Where.Expr, place.Column)
		if !ok {
			return nil, false, place, nil, fmt.Errorf("persist: statement spans shards")
		}
		id, ok, err := intValue(expr, binds)
		if err != nil {
			return nil, false, place, nil, err
		}
		if !ok {
			return nil, false, place, nil, fmt.Errorf("persist: statement has no shard key")
		}
		return []int64{id}, false, place, nil, nil
	case *sqlparser.Delete:
		name, ok := singleTable(n.TableExprs)
		if !ok {
			return nil, false, persist.TablePlacement{}, nil, fmt.Errorf("persist: statement spans shards")
		}
		place, ok := findPlace(places, db, name)
		if !ok {
			return nil, false, persist.TablePlacement{}, nil, nil
		}
		if n.Where == nil {
			return nil, false, place, nil, fmt.Errorf("persist: statement spans shards")
		}
		expr, ok := equality(n.Where.Expr, place.Column)
		if !ok {
			return nil, false, place, nil, fmt.Errorf("persist: statement spans shards")
		}
		id, ok, err := intValue(expr, binds)
		if err != nil || !ok {
			return nil, false, place, nil, fmt.Errorf("persist: statement has no shard key")
		}
		return []int64{id}, false, place, nil, nil
	default:
		return nil, false, persist.TablePlacement{}, nil, nil
	}
}

func findPlace(places []persist.TablePlacement, db string, name sqlparser.TableName) (persist.TablePlacement, bool) {
	qual := db
	if !name.DbQualifier.IsEmpty() {
		qual = name.DbQualifier.String()
	}
	key := strings.ToLower(qual) + "." + strings.ToLower(name.Name.String())
	for _, p := range places {
		if p.Key() == key {
			return p, true
		}
	}
	return persist.TablePlacement{}, false
}

func singleTable(exprs sqlparser.TableExprs) (sqlparser.TableName, bool) {
	if len(exprs) != 1 {
		return sqlparser.TableName{}, false
	}
	aliased, ok := exprs[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return sqlparser.TableName{}, false
	}
	name, ok := aliased.Expr.(sqlparser.TableName)
	return name, ok
}

func equality(expr sqlparser.Expr, column string) (sqlparser.Expr, bool) {
	switch n := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if n.Operator != sqlparser.EqualStr {
			return nil, false
		}
		if col, ok := n.Left.(*sqlparser.ColName); ok && strings.EqualFold(col.Name.String(), column) {
			return n.Right, true
		}
		if col, ok := n.Right.(*sqlparser.ColName); ok && strings.EqualFold(col.Name.String(), column) {
			return n.Left, true
		}
		return nil, false
	case *sqlparser.AndExpr:
		if e, ok := equality(n.Left, column); ok {
			return e, true
		}
		return equality(n.Right, column)
	case *sqlparser.ParenExpr:
		return equality(n.Expr, column)
	default:
		return nil, false
	}
}

func insertIDs(ins *sqlparser.Insert, column string, binds []persist.ForwardBind) ([]int64, error) {
	idx := columnIndex(ins.Columns, column)
	if idx < 0 {
		return nil, fmt.Errorf("persist: sharded insert must include %s", column)
	}
	rows, err := insertRows(ins.Rows)
	if err != nil {
		return nil, err
	}
	ids := make([]int64, 0, len(rows))
	for _, row := range rows {
		if idx >= len(row) {
			return nil, fmt.Errorf("persist: sharded insert must include %s", column)
		}
		id, ok, err := intValue(row[idx], binds)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("persist: sharded insert must include %s", column)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func insertChecks(ins *sqlparser.Insert, place persist.TablePlacement, binds []persist.ForwardBind) ([]routedValue, error) {
	if len(place.Check) == 0 {
		return nil, nil
	}
	rows, err := insertRows(ins.Rows)
	if err != nil {
		return nil, err
	}
	var out []routedValue
	for _, column := range place.Check {
		idx := columnIndex(ins.Columns, column)
		if idx < 0 {
			continue
		}
		for _, row := range rows {
			if idx >= len(row) {
				continue
			}
			text, quote, ok, err := scalarValue(row[idx], binds)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			out = append(out, routedValue{Column: column, Text: text, Quote: quote})
		}
	}
	return out, nil
}

func insertRows(rows sqlparser.InsertRows) ([]sqlparser.ValTuple, error) {
	switch n := rows.(type) {
	case sqlparser.Values:
		return []sqlparser.ValTuple(n), nil
	case *sqlparser.Values:
		return []sqlparser.ValTuple(*n), nil
	case sqlparser.AliasedValues:
		return []sqlparser.ValTuple(n.Values), nil
	case *sqlparser.AliasedValues:
		return []sqlparser.ValTuple(n.Values), nil
	default:
		return nil, fmt.Errorf("persist: sharded insert must list its values")
	}
}

func columnIndex(cols sqlparser.Columns, name string) int {
	for i, col := range cols {
		if strings.EqualFold(col.String(), name) {
			return i
		}
	}
	return -1
}

func intValue(expr sqlparser.Expr, binds []persist.ForwardBind) (int64, bool, error) {
	text, _, ok, err := scalarValue(expr, binds)
	if err != nil || !ok {
		return 0, false, err
	}
	id, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return 0, false, nil
	}
	return id, true, nil
}

func scalarValue(expr sqlparser.Expr, binds []persist.ForwardBind) (string, bool, bool, error) {
	val, ok := expr.(*sqlparser.SQLVal)
	if !ok {
		return "", false, false, nil
	}
	switch val.Type {
	case sqlparser.IntVal:
		return string(val.Val), false, true, nil
	case sqlparser.StrVal:
		return string(val.Val), true, true, nil
	case sqlparser.ValArg:
		name := strings.TrimPrefix(string(val.Val), ":")
		for _, bind := range binds {
			if bind.Name != name {
				continue
			}
			if len(bind.Value) == 0 {
				return "", false, false, nil
			}
			text := string(bind.Value)
			quote := bind.Type != 0 && !intBind(bind.Type)
			return text, quote, true, nil
		}
		return "", false, false, fmt.Errorf("persist: missing bind %s", name)
	default:
		return "", false, false, nil
	}
}

func intBind(typ int32) bool {
	switch query.Type(typ) {
	case query.Type_INT8, query.Type_UINT8, query.Type_INT16, query.Type_UINT16,
		query.Type_INT24, query.Type_UINT24, query.Type_INT32, query.Type_UINT32,
		query.Type_INT64, query.Type_UINT64:
		return true
	default:
		return false
	}
}

// parseShardStmt recognizes SHARD TABLE db.table BY column [CHECK col].
func parseShardStmt(query string) (persist.TablePlacement, bool) {
	q := strings.TrimSpace(query)
	if strings.HasSuffix(q, ";") {
		q = strings.TrimSpace(strings.TrimSuffix(q, ";"))
	}
	fields, err := splitAdmin(q)
	if err != nil || len(fields) < 5 {
		return persist.TablePlacement{}, false
	}
	if !strings.EqualFold(fields[0], "SHARD") || !strings.EqualFold(fields[1], "TABLE") || !strings.EqualFold(fields[3], "BY") {
		return persist.TablePlacement{}, false
	}
	db, table, ok := splitQual(fields[2])
	if !ok || fields[4] == "" {
		return persist.TablePlacement{}, false
	}
	p := persist.TablePlacement{DB: db, Table: table, Column: fields[4]}
	if len(fields) == 5 {
		return p, true
	}
	if len(fields) == 7 && strings.EqualFold(fields[5], "CHECK") && fields[6] != "" {
		p.Check = []string{fields[6]}
		return p, true
	}
	return persist.TablePlacement{}, false
}

func splitQual(name string) (string, string, bool) {
	db, table, ok := strings.Cut(name, ".")
	if !ok || db == "" || table == "" || strings.Contains(table, ".") {
		return "", "", false
	}
	return db, table, true
}
