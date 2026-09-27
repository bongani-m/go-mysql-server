package main

import (
	"fmt"
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
	}, places, nil, 2, -1)
	require.NoError(t, err)
	require.Equal(t, RouteShard, one.Kind)
	require.Equal(t, 0, one.Shard)

	tx, err := RouteQuery("UPDATE accounts SET status = ? WHERE id = ?", "stress", []persist.ForwardBind{
		{Name: "v1", Type: int32(query.Type_INT64), Value: []byte("1")},
		{Name: "v2", Type: int32(query.Type_INT64), Value: []byte(itoa(left))},
	}, places, nil, 2, -1)
	require.NoError(t, err)
	require.Equal(t, 0, tx.Shard)

	_, err = RouteQuery("UPDATE accounts SET status = ? WHERE id = ?", "stress", []persist.ForwardBind{
		{Name: "v1", Type: int32(query.Type_INT64), Value: []byte("1")},
		{Name: "v2", Type: int32(query.Type_INT64), Value: []byte(itoa(right))},
	}, places, nil, 2, tx.Shard)
	require.Error(t, err)
	require.Contains(t, err.Error(), "transaction spans shards")

	note, err := RouteQuery("INSERT INTO notes (account_id, body, created_at) VALUES (?, ?, ?)", "stress", []persist.ForwardBind{
		{Name: "v1", Type: int32(query.Type_INT64), Value: []byte(itoa(left))},
		{Name: "v2", Type: int32(query.Type_VARCHAR), Value: []byte("tx")},
		{Name: "v3", Type: int32(query.Type_DATETIME), Value: []byte("2026-01-01 00:00:00")},
	}, places, nil, 2, tx.Shard)
	require.NoError(t, err)
	require.Equal(t, RouteShard, note.Kind)
	require.Equal(t, 0, note.Shard)
}

func TestRouteScatterEmailAndDuplicateCheck(t *testing.T) {
	places := testPlaces()
	email, err := RouteQuery("SELECT id FROM accounts WHERE email = ?", "stress", []persist.ForwardBind{
		{Name: "v1", Type: int32(query.Type_VARCHAR), Value: []byte("user1@example.com")},
	}, places, nil, 2, -1)
	require.NoError(t, err)
	require.Equal(t, RouteScatter, email.Kind)

	ins, err := RouteQuery(
		"INSERT INTO accounts (id, email, name, status, created_at) VALUES (1, 'user1@example.com', 'User 1', 1, '2026-01-01')",
		"stress", nil, places, nil, 2, -1)
	require.NoError(t, err)
	require.Equal(t, RouteShard, ins.Kind)
	require.Equal(t, persist.ShardIndex(1, 2), ins.Shard)
	require.Equal(t, []routedValue{{Column: "email", Text: "user1@example.com", Quote: true}}, ins.Checks)

	_, err = RouteQuery(
		"INSERT INTO accounts (id, email) VALUES (1, 'a@b.c'), (2, 'c@d.e')",
		"stress", nil, places, nil, 2, -1)
	if persist.ShardIndex(1, 2) != persist.ShardIndex(2, 2) {
		require.Error(t, err)
		require.Contains(t, err.Error(), "spans shards")
	}
}

func TestRouteDDLAndPlacement(t *testing.T) {
	ddl, err := RouteQuery("CREATE TABLE accounts (id BIGINT NOT NULL, PRIMARY KEY (id))", "stress", nil, nil, nil, 2, -1)
	require.NoError(t, err)
	require.Equal(t, RouteDDL, ddl.Kind)

	place, err := RouteQuery("SHARD TABLE stress.accounts BY id CHECK email", "stress", nil, nil, nil, 2, -1)
	require.NoError(t, err)
	require.Equal(t, RoutePlace, place.Kind)
	require.Equal(t, "id", place.Place.Column)
	require.Equal(t, []string{"email"}, place.Place.Check)
}

func TestCheckQueriesGroupsQuotesAndChunks(t *testing.T) {
	place := persist.TablePlacement{DB: "stress", Table: "accounts"}
	queries, dup := checkQueries(place, []routedValue{
		{Column: "email", Text: "user1@example.com", Quote: true},
		{Column: "email", Text: "o'reilly@b.c", Quote: true},
	})
	require.Nil(t, dup)
	require.Equal(t, []checkQuery{{
		Column: "email",
		SQL:    "SELECT email FROM stress.accounts WHERE email IN ('user1@example.com','o''reilly@b.c')",
	}}, queries)

	_, dup = checkQueries(place, []routedValue{
		{Column: "email", Text: "a@b.c", Quote: true},
		{Column: "email", Text: "a@b.c", Quote: true},
	})
	require.NotNil(t, dup)
	require.Equal(t, "a@b.c", dup.Text)
	require.Equal(t, "email", dup.Column)

	many := make([]routedValue, checkBatch+1)
	for i := range many {
		many[i] = routedValue{Column: "email", Text: fmt.Sprintf("u%d@b.c", i), Quote: true}
	}
	queries, dup = checkQueries(place, many)
	require.Nil(t, dup)
	require.Len(t, queries, 2)
	require.Equal(t, "email", queries[0].Column)
	require.Equal(t, "email", queries[1].Column)
}

func itoa(n int64) string {
	return strconv.FormatInt(n, 10)
}

func testRanges() ([]persist.TablePlacement, []persist.KeyRange) {
	places := []persist.TablePlacement{{DB: "stress", Table: "accounts", Column: "id", Check: []string{"email"}}}
	mid := persist.KeySuccessor(persist.EncodeIntKey(100))
	ranges := []persist.KeyRange{
		{ID: "lo", DB: "stress", Table: "accounts", End: mid, Group: "g0", State: persist.RangeActive},
		{ID: "hi", DB: "stress", Table: "accounts", Start: mid, Group: "g1", State: persist.RangeActive},
	}
	return places, ranges
}

func TestRouteRangeEqualityBetweenAndScatter(t *testing.T) {
	places, ranges := testRanges()
	one, err := RouteQuery("SELECT email FROM accounts WHERE id = 40", "stress", nil, places, ranges, 2, -1)
	require.NoError(t, err)
	require.Equal(t, RouteShard, one.Kind)
	require.Equal(t, []string{"g0"}, one.Groups)
	require.True(t, one.Ranged)

	hi, err := RouteQuery("SELECT email FROM accounts WHERE id = 140", "stress", nil, places, ranges, 2, -1)
	require.NoError(t, err)
	require.Equal(t, []string{"g1"}, hi.Groups)

	span, err := RouteQuery("SELECT email FROM accounts WHERE id >= 90 AND id < 110", "stress", nil, places, ranges, 2, -1)
	require.NoError(t, err)
	require.Equal(t, RouteScatter, span.Kind)
	require.Equal(t, []string{"g0", "g1"}, span.Groups)

	between, err := RouteQuery("SELECT email FROM accounts WHERE id BETWEEN 1 AND 50", "stress", nil, places, ranges, 2, -1)
	require.NoError(t, err)
	require.Equal(t, RouteShard, between.Kind)
	require.Equal(t, []string{"g0"}, between.Groups)

	all, err := RouteQuery("SELECT email FROM accounts WHERE email = 'a@b.c'", "stress", nil, places, ranges, 2, -1)
	require.NoError(t, err)
	require.Equal(t, RouteScatter, all.Kind)
	require.Equal(t, []string{"g0", "g1"}, all.Groups)

	_, err = RouteQuery("UPDATE accounts SET status = 1 WHERE id >= 90 AND id < 110", "stress", nil, places, ranges, 2, -1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "spans shards")

	_, err = RouteQuery("INSERT INTO accounts (id, email) VALUES (1, 'a@b.c'), (140, 'c@d.e')", "stress", nil, places, ranges, 2, -1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "spans shards")

	// A second statement in the same transaction is allowed. Commit uses two phases.
	left, err := RouteQuery("UPDATE accounts SET status = 1 WHERE id = 40", "stress", nil, places, ranges, 2, -1)
	require.NoError(t, err)
	right, err := RouteQuery("UPDATE accounts SET status = 1 WHERE id = 140", "stress", nil, places, ranges, 2, -1)
	require.NoError(t, err)
	require.Equal(t, []string{"g0"}, left.Groups)
	require.Equal(t, []string{"g1"}, right.Groups)

	created, err := RouteQuery("SHARD TABLE stress.accounts BY id RANGE", "stress", nil, nil, nil, 2, -1)
	require.NoError(t, err)
	require.Equal(t, RoutePlace, created.Kind)
	require.True(t, created.Ranged)
	require.Equal(t, "id", created.Place.Column)
	require.False(t, created.SpanSet)

	bounded, err := RouteQuery("SHARD TABLE stress.accounts BY id CHECK email RANGE END 2501 GROUP g0 PEER n1 10.0.0.1:7101 10.0.0.1:7102", "stress", nil, nil, nil, 2, -1)
	require.NoError(t, err)
	require.True(t, bounded.SpanSet)
	require.Equal(t, "g0", bounded.Span.Group)
	require.Equal(t, persist.EncodeIntKey(2501), bounded.Span.End)
	require.Empty(t, bounded.Span.Start)
	require.Equal(t, []string{"email"}, bounded.Place.Check)
	require.Equal(t, "10.0.0.1:7102", bounded.Span.Peers[0].Forward)
}
