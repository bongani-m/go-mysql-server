package main

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/persist"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

func testPlaces() []persist.TablePlacement {
	return []persist.TablePlacement{
		{DB: "stress", Table: "accounts", Column: "id", Check: []string{"email"}},
		{DB: "stress", Table: "notes", Column: "account_id"},
	}
}

func TestRoutePointReadAndTransaction(t *testing.T) {
	places := testPlaces()
	var left, right int64
	for id := int64(1); id < 200; id++ {
		if persist.ShardIndex(id, 2) == 0 && left == 0 {
			left = id
		}
		if persist.ShardIndex(id, 2) == 1 && right == 0 {
			right = id
		}
	}
	require.NotZero(t, left)
	require.NotZero(t, right)

	one, err := RouteQuery("SELECT email, status FROM accounts WHERE id = ?", "stress", []persist.ForwardBind{
		{Name: "v1", Type: int32(query.Type_INT64), Value: []byte(itoa(left))},
	}, places, 2, -1)
	require.NoError(t, err)
	require.Equal(t, RouteShard, one.Kind)
	require.Equal(t, 0, one.Shard)

	tx, err := RouteQuery("UPDATE accounts SET status = ? WHERE id = ?", "stress", []persist.ForwardBind{
		{Name: "v1", Type: int32(query.Type_INT64), Value: []byte("1")},
		{Name: "v2", Type: int32(query.Type_INT64), Value: []byte(itoa(left))},
	}, places, 2, -1)
	require.NoError(t, err)
	require.Equal(t, 0, tx.Shard)

	_, err = RouteQuery("UPDATE accounts SET status = ? WHERE id = ?", "stress", []persist.ForwardBind{
		{Name: "v1", Type: int32(query.Type_INT64), Value: []byte("1")},
		{Name: "v2", Type: int32(query.Type_INT64), Value: []byte(itoa(right))},
	}, places, 2, tx.Shard)
	require.Error(t, err)
	require.Contains(t, err.Error(), "transaction spans shards")

	note, err := RouteQuery("INSERT INTO notes (account_id, body, created_at) VALUES (?, ?, ?)", "stress", []persist.ForwardBind{
		{Name: "v1", Type: int32(query.Type_INT64), Value: []byte(itoa(left))},
		{Name: "v2", Type: int32(query.Type_VARCHAR), Value: []byte("tx")},
		{Name: "v3", Type: int32(query.Type_DATETIME), Value: []byte("2026-01-01 00:00:00")},
	}, places, 2, tx.Shard)
	require.NoError(t, err)
	require.Equal(t, RouteShard, note.Kind)
	require.Equal(t, 0, note.Shard)
}

func TestRouteScatterEmailAndDuplicateCheck(t *testing.T) {
	places := testPlaces()
	email, err := RouteQuery("SELECT id FROM accounts WHERE email = ?", "stress", []persist.ForwardBind{
		{Name: "v1", Type: int32(query.Type_VARCHAR), Value: []byte("user1@example.com")},
	}, places, 2, -1)
	require.NoError(t, err)
	require.Equal(t, RouteScatter, email.Kind)

	ins, err := RouteQuery(
		"INSERT INTO accounts (id, email, name, status, created_at) VALUES (1, 'user1@example.com', 'User 1', 1, '2026-01-01')",
		"stress", nil, places, 2, -1)
	require.NoError(t, err)
	require.Equal(t, RouteShard, ins.Kind)
	require.Equal(t, persist.ShardIndex(1, 2), ins.Shard)
	require.Equal(t, []routedValue{{Column: "email", Text: "user1@example.com", Quote: true}}, ins.Checks)

	_, err = RouteQuery(
		"INSERT INTO accounts (id, email) VALUES (1, 'a@b.c'), (2, 'c@d.e')",
		"stress", nil, places, 2, -1)
	if persist.ShardIndex(1, 2) != persist.ShardIndex(2, 2) {
		require.Error(t, err)
		require.Contains(t, err.Error(), "spans shards")
	}
}

func TestRouteDDLAndPlacement(t *testing.T) {
	ddl, err := RouteQuery("CREATE TABLE accounts (id BIGINT NOT NULL, PRIMARY KEY (id))", "stress", nil, nil, 2, -1)
	require.NoError(t, err)
	require.Equal(t, RouteDDL, ddl.Kind)

	place, err := RouteQuery("SHARD TABLE stress.accounts BY id CHECK email", "stress", nil, nil, 2, -1)
	require.NoError(t, err)
	require.Equal(t, RoutePlace, place.Kind)
	require.Equal(t, "id", place.Place.Column)
	require.Equal(t, []string{"email"}, place.Place.Check)
}

func itoa(n int64) string {
	return strconv.FormatInt(n, 10)
}
