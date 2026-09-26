package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/persist"
)

// partConfig is the shard this process stores and how to reach the others.
type partConfig struct {
	Index    int
	Count    int
	Contacts []string
}

// loadPartConfig reads the partitioned layout. A process without
// GMS_META_ADDR is a single Raft group.
func loadPartConfig() (*partConfig, error) {
	if strings.TrimSpace(os.Getenv("GMS_META_ADDR")) == "" {
		return nil, nil
	}
	count, err := strconv.Atoi(strings.TrimSpace(os.Getenv("GMS_SHARD_COUNT")))
	if err != nil || count < 2 {
		return nil, fmt.Errorf("GMS_SHARD_COUNT: want an integer greater than 1")
	}
	index, err := strconv.Atoi(strings.TrimSpace(os.Getenv("GMS_SHARD_INDEX")))
	if err != nil || index < 0 || index >= count {
		return nil, fmt.Errorf("GMS_SHARD_INDEX: want an index in 0..%d", count-1)
	}
	contacts, err := parseShardContacts(os.Getenv("GMS_SHARD_FORWARD"), count)
	if err != nil {
		return nil, err
	}
	return &partConfig{Index: index, Count: count, Contacts: contacts}, nil
}

func parseShardContacts(raw string, count int) ([]string, error) {
	contacts := make([]string, count)
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("GMS_SHARD_FORWARD is empty")
	}
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		idxRaw, addr, ok := strings.Cut(part, "=")
		if !ok || addr == "" {
			return nil, fmt.Errorf("GMS_SHARD_FORWARD: %q must be index=host:port", part)
		}
		idx, err := strconv.Atoi(idxRaw)
		if err != nil || idx < 0 || idx >= count {
			return nil, fmt.Errorf("GMS_SHARD_FORWARD: index %q", idxRaw)
		}
		contacts[idx] = addr
	}
	for i, addr := range contacts {
		if addr == "" {
			return nil, fmt.Errorf("GMS_SHARD_FORWARD: shard %d is missing", i)
		}
	}
	return contacts, nil
}

type partPin struct {
	inTx  bool
	shard int
}

// partHandler routes a statement to the shard that owns its key.
// Catalog changes go to meta and to every shard. The engine-wide read-only
// gate stays off: this node can lead its shard and follow meta.
type partHandler struct {
	*forwardHandler
	meta *persist.Store
	cfg  *partConfig
	mu   sync.Mutex
	pins map[uint32]*partPin
	// clients is one forward connection per MySQL session per remote address.
	// A process-wide client would queue every session on one socket.
	clients map[uint32]map[string]*fwdConn
	places  placeCache
}

// fwdConn is a pooled forward connection and the node id used to close it.
type fwdConn struct {
	client *persist.ForwardClient
	node   string
}

// placeCache is the placement list last read at a meta FSM index.
type placeCache struct {
	index uint64
	list  []persist.TablePlacement
	ok    bool
}

func newPartHandler(inner *forwardHandler, meta *persist.Store, cfg *partConfig) mysql.Handler {
	return &partHandler{
		forwardHandler: inner,
		meta:           meta,
		cfg:            cfg,
		pins:           make(map[uint32]*partPin),
		clients:        make(map[uint32]map[string]*fwdConn),
	}
}

func (h *partHandler) ConnectionClosed(c *mysql.Conn) {
	h.dropSession(c.ConnectionID)
	h.forwardHandler.ConnectionClosed(c)
}

func (h *partHandler) ComResetConnection(c *mysql.Conn) error {
	h.dropSession(c.ConnectionID)
	return h.forwardHandler.ComResetConnection(c)
}

func (h *partHandler) dropSession(id uint32) {
	h.mu.Lock()
	delete(h.pins, id)
	by := h.clients[id]
	delete(h.clients, id)
	h.mu.Unlock()
	for _, conn := range by {
		conn.client.Close(conn.node, uint64(id))
	}
}

func (h *partHandler) ComQuery(ctx context.Context, c *mysql.Conn, query string, callback mysql.ResultSpoolFn) error {
	return h.dispatch(ctx, c, query, nil, callback)
}

func (h *partHandler) ComMultiQuery(ctx context.Context, c *mysql.Conn, query string, callback mysql.ResultSpoolFn) (string, error) {
	first, rest := splitFirst(query)
	if err := h.dispatch(ctx, c, first, nil, callback); err != nil {
		return "", err
	}
	return rest, nil
}

func (h *partHandler) ComStmtExecute(ctx context.Context, c *mysql.Conn, prepare *mysql.PrepareData, callback func(*sqltypes.Result) error) error {
	var binds []persist.ForwardBind
	for name, bind := range prepare.BindVars {
		binds = append(binds, persist.ForwardBind{
			Name:  name,
			Type:  int32(bind.Type),
			Value: append([]byte(nil), bind.Value...),
		})
	}
	return h.dispatch(ctx, c, prepare.PrepareStmt, binds, func(res *sqltypes.Result, _ bool) error {
		return callback(res)
	})
}

func (h *partHandler) dispatch(ctx context.Context, c *mysql.Conn, query string, binds []persist.ForwardBind, callback mysql.ResultSpoolFn) error {
	if _, ok := parseAdmin(query); ok {
		return h.forwardHandler.dispatch(ctx, c, query, binds, callback)
	}
	places, err := h.placements()
	if err != nil {
		return err
	}
	pin := h.pin(c)
	pinned := -1
	if pin.inTx && pin.shard >= 0 {
		pinned = pin.shard
	}
	route, err := RouteQuery(query, currentDB(h.Handler, c), binds, places, h.cfg.Count, pinned)
	if err != nil {
		return mysql.NewSQLError(mysql.ERUnknownError, "HY000", "%s", err.Error())
	}
	switch route.Kind {
	case RoutePlace:
		if err := h.savePlacement(c, route.Place); err != nil {
			return err
		}
		return callback(&sqltypes.Result{}, false)
	case RouteDDL:
		if err := h.broadcastDDL(c, query); err != nil {
			return err
		}
		return callback(&sqltypes.Result{}, false)
	case RouteScatter:
		reply, err := h.scatter(c, query, binds)
		if err != nil {
			return err
		}
		return spoolReply(reply, callback)
	case RouteShard:
		if err := h.rejectDups(c, route); err != nil {
			return err
		}
		if pin.inTx {
			pin.shard = route.Shard
		}
		if route.Shard == h.cfg.Index {
			return h.forwardHandler.dispatch(ctx, c, query, binds, callback)
		}
		reply, err := h.execShard(c, route.Shard, query, binds)
		if err != nil {
			return err
		}
		return spoolReply(reply, callback)
	default:
		switch txKind(query) {
		case txBegin:
			pin.inTx = true
			pin.shard = -1
		case txEnd:
			pin.inTx = false
			pin.shard = -1
		}
		return h.forwardHandler.dispatch(ctx, c, query, binds, callback)
	}
}

func (h *partHandler) pin(c *mysql.Conn) *partPin {
	h.mu.Lock()
	defer h.mu.Unlock()
	pin := h.pins[c.ConnectionID]
	if pin == nil {
		pin = &partPin{shard: -1}
		h.pins[c.ConnectionID] = pin
	}
	return pin
}

func (h *partHandler) placements() ([]persist.TablePlacement, error) {
	index := h.meta.FSMApplied()
	h.mu.Lock()
	if h.places.ok && h.places.index == index {
		list := append([]persist.TablePlacement(nil), h.places.list...)
		h.mu.Unlock()
		return list, nil
	}
	h.mu.Unlock()

	list, err := h.meta.Placements()
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	if h.meta.FSMApplied() == index {
		h.places = placeCache{
			index: index,
			list:  append([]persist.TablePlacement(nil), list...),
			ok:    true,
		}
	}
	h.mu.Unlock()
	return list, nil
}

func (h *partHandler) savePlacement(c *mysql.Conn, p persist.TablePlacement) error {
	if h.meta.IsLeader() {
		return h.meta.SavePlacement(p)
	}
	reply, err := h.call(c, h.meta, "", queryReq(h.meta, placementSQL(p), nil))
	if err != nil {
		return err
	}
	if reply.Err != "" {
		return mysql.NewSQLError(mysql.ERUnknownError, "HY000", "%s", reply.Err)
	}
	if reply.Index > 0 {
		return h.meta.WaitApplied(reply.Index, h.meta.ApplyTimeout())
	}
	return nil
}

func placementSQL(p persist.TablePlacement) string {
	q := fmt.Sprintf("SHARD TABLE %s.%s BY %s", p.DB, p.Table, p.Column)
	if len(p.Check) > 0 {
		q += " CHECK " + p.Check[0]
	}
	return q
}

// broadcastDDL applies query to meta, then to every shard leader.
func (h *partHandler) broadcastDDL(c *mysql.Conn, query string) error {
	if err := h.execMeta(c, query); err != nil {
		return err
	}
	for shard := 0; shard < h.cfg.Count; shard++ {
		reply, err := h.execShard(c, shard, query, nil)
		if err != nil {
			return err
		}
		if reply.Err != "" {
			return mysql.NewSQLError(mysql.ERUnknownError, "HY000", "%s", reply.Err)
		}
		if shard == h.cfg.Index && reply.Index > 0 {
			if err := h.store.WaitApplied(reply.Index, h.store.ApplyTimeout()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *partHandler) execMeta(c *mysql.Conn, query string) error {
	reply, err := h.call(c, h.meta, "", h.request(c, h.meta, query, nil))
	if err != nil {
		return err
	}
	if reply.Err != "" {
		return mysql.NewSQLError(mysql.ERUnknownError, "HY000", "%s", reply.Err)
	}
	if !h.meta.IsLeader() && reply.Index > 0 {
		return h.meta.WaitApplied(reply.Index, h.meta.ApplyTimeout())
	}
	return nil
}

func (h *partHandler) execShard(c *mysql.Conn, shard int, query string, binds []persist.ForwardBind) (persist.ForwardReply, error) {
	addr, err := h.leaderForward(c, h.cfg.Contacts[shard])
	if err != nil {
		return persist.ForwardReply{}, err
	}
	reply, err := h.call(c, h.store, addr, h.request(c, h.store, query, binds))
	if err != nil {
		return persist.ForwardReply{}, err
	}
	if reply.Err != "" {
		return persist.ForwardReply{}, mysql.NewSQLError(mysql.ERUnknownError, "HY000", "%s", reply.Err)
	}
	return reply, nil
}

func (h *partHandler) scatter(c *mysql.Conn, query string, binds []persist.ForwardBind) (persist.ForwardReply, error) {
	req := h.request(c, h.store, query, binds)
	return scatterCalls(h.cfg.Contacts, h.store.LocalForwardAddr(),
		func(string) (persist.ForwardReply, error) {
			return checkedReply(h.store.ExecForward(req))
		},
		func(addr string) (persist.ForwardReply, error) {
			return checkedReply(h.execPooled(c, h.store, addr, req))
		},
	)
}

// checkBatch is how many unique values one scatter probes. A seed insert of
// 100 rows fits in one query.
const checkBatch = 200

type checkQuery struct {
	Column string
	SQL    string
}

// checkQueries builds one lookup per column. A value repeated in this
// statement is a duplicate before any shard is asked.
func checkQueries(place persist.TablePlacement, checks []routedValue) ([]checkQuery, *routedValue) {
	if len(checks) == 0 {
		return nil, nil
	}
	type group struct {
		vals []routedValue
		seen map[string]struct{}
	}
	var order []string
	groups := make(map[string]*group)
	for _, check := range checks {
		g := groups[check.Column]
		if g == nil {
			g = &group{seen: make(map[string]struct{})}
			groups[check.Column] = g
			order = append(order, check.Column)
		}
		if _, ok := g.seen[check.Text]; ok {
			dup := check
			return nil, &dup
		}
		g.seen[check.Text] = struct{}{}
		g.vals = append(g.vals, check)
	}
	var queries []checkQuery
	for _, column := range order {
		vals := groups[column].vals
		for from := 0; from < len(vals); from += checkBatch {
			to := from + checkBatch
			if to > len(vals) {
				to = len(vals)
			}
			var b strings.Builder
			b.WriteString("SELECT ")
			b.WriteString(column)
			b.WriteString(" FROM ")
			b.WriteString(place.DB)
			b.WriteByte('.')
			b.WriteString(place.Table)
			b.WriteString(" WHERE ")
			b.WriteString(column)
			b.WriteString(" IN (")
			for i, v := range vals[from:to] {
				if i > 0 {
					b.WriteByte(',')
				}
				b.WriteString(sqlLiteral(v))
			}
			b.WriteByte(')')
			queries = append(queries, checkQuery{Column: column, SQL: b.String()})
		}
	}
	return queries, nil
}

func sqlLiteral(v routedValue) string {
	if !v.Quote {
		return v.Text
	}
	return "'" + strings.ReplaceAll(v.Text, "'", "''") + "'"
}

func (h *partHandler) rejectDups(c *mysql.Conn, route Route) error {
	queries, dup := checkQueries(route.Place, route.Checks)
	if dup != nil {
		return dupEntry(dup.Text, dup.Column)
	}
	for _, q := range queries {
		reply, err := h.scatter(c, q.SQL, nil)
		if err != nil {
			return err
		}
		if len(reply.Rows) == 0 || len(reply.Rows[0]) == 0 || reply.Rows[0][0].Null {
			continue
		}
		return dupEntry(string(reply.Rows[0][0].Raw), q.Column)
	}
	return nil
}

func dupEntry(text, column string) error {
	return mysql.NewSQLError(mysql.ERDupEntry, "23000", "Duplicate entry '%s' for key '%s'", text, column)
}

// leaderForward resolves the leader's forward address from one member of the group.
func (h *partHandler) leaderForward(c *mysql.Conn, contact string) (string, error) {
	reply, err := h.call(c, h.store, contact, queryReq(h.store, "SHOW RAFT STATUS", nil))
	if err != nil {
		return "", err
	}
	if reply.Err != "" {
		return "", fmt.Errorf("%s", reply.Err)
	}
	if len(reply.Rows) == 0 || len(reply.Rows[0]) < 2 || len(reply.Rows[0][1].Raw) == 0 {
		return "", fmt.Errorf("persist: shard leader is unknown")
	}
	leader := string(reply.Rows[0][1].Raw)
	host, _, err := net.SplitHostPort(leader)
	if err != nil {
		return "", err
	}
	_, port, err := net.SplitHostPort(contact)
	if err != nil {
		return "", err
	}
	return net.JoinHostPort(host, port), nil
}

// call runs req on addr. An empty addr is the store's current leader. The
// local forward listener runs in-process. Any other address reuses the
// session's pooled connection.
func (h *partHandler) call(c *mysql.Conn, store *persist.Store, addr string, req persist.ForwardRequest) (persist.ForwardReply, error) {
	if addr == "" {
		var err error
		addr, err = store.LeaderForwardAddr()
		if err != nil {
			return persist.ForwardReply{}, err
		}
	}
	if local := store.LocalForwardAddr(); local != "" && addr == local {
		return store.ExecForward(req)
	}
	return h.execPooled(c, store, addr, req)
}

func (h *partHandler) execPooled(c *mysql.Conn, store *persist.Store, addr string, req persist.ForwardRequest) (persist.ForwardReply, error) {
	id := uint32(0)
	if c != nil {
		id = c.ConnectionID
	}
	conn, err := h.clientFor(id, store, addr)
	if err != nil {
		return persist.ForwardReply{}, err
	}
	reply, err := conn.client.Exec(req, store.ApplyTimeout())
	if err != nil {
		h.retire(id, addr)
		return persist.ForwardReply{}, err
	}
	return reply, nil
}

func (h *partHandler) clientFor(id uint32, store *persist.Store, addr string) (*fwdConn, error) {
	h.mu.Lock()
	if by := h.clients[id]; by != nil {
		if conn := by[addr]; conn != nil {
			h.mu.Unlock()
			return conn, nil
		}
	}
	h.mu.Unlock()

	client, err := store.DialForwardAddr(addr)
	if err != nil {
		return nil, err
	}
	conn := &fwdConn{client: client, node: store.NodeID()}

	h.mu.Lock()
	if h.clients[id] == nil {
		h.clients[id] = make(map[string]*fwdConn)
	}
	if existing := h.clients[id][addr]; existing != nil {
		h.mu.Unlock()
		client.Close(conn.node, uint64(id))
		return existing, nil
	}
	h.clients[id][addr] = conn
	h.mu.Unlock()
	return conn, nil
}

func (h *partHandler) retire(id uint32, addr string) {
	h.mu.Lock()
	by := h.clients[id]
	var conn *fwdConn
	if by != nil {
		conn = by[addr]
		delete(by, addr)
	}
	h.mu.Unlock()
	if conn != nil {
		conn.client.Close(conn.node, uint64(id))
	}
}

func checkedReply(reply persist.ForwardReply, err error) (persist.ForwardReply, error) {
	if err != nil {
		return persist.ForwardReply{}, err
	}
	if reply.Err != "" {
		return persist.ForwardReply{}, mysql.NewSQLError(mysql.ERUnknownError, "HY000", "%s", reply.Err)
	}
	return reply, nil
}

// scatterCalls runs one hop per addr at the same time. localAddr is executed
// with local and is not passed to remote. Rows are merged in addr order.
func scatterCalls(addrs []string, localAddr string, local, remote func(addr string) (persist.ForwardReply, error)) (persist.ForwardReply, error) {
	return scatterFanout(addrs, func(addr string) (persist.ForwardReply, error) {
		if localAddr != "" && addr == localAddr {
			return local(addr)
		}
		return remote(addr)
	})
}

func scatterFanout(addrs []string, call func(addr string) (persist.ForwardReply, error)) (persist.ForwardReply, error) {
	replies := make([]persist.ForwardReply, len(addrs))
	errs := make([]error, len(addrs))
	var wg sync.WaitGroup
	for i, addr := range addrs {
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			reply, err := call(addr)
			if err != nil {
				errs[i] = err
				return
			}
			replies[i] = reply
		}(i, addr)
	}
	wg.Wait()
	var merged persist.ForwardReply
	for i := range addrs {
		if errs[i] != nil {
			return persist.ForwardReply{}, errs[i]
		}
		merged = mergeReply(merged, replies[i])
	}
	return merged, nil
}

func queryReq(store *persist.Store, query string, binds []persist.ForwardBind) persist.ForwardRequest {
	return persist.ForwardRequest{
		Node:  store.NodeID(),
		Query: query,
		Binds: binds,
	}
}

func (h *partHandler) request(c *mysql.Conn, store *persist.Store, query string, binds []persist.ForwardBind) persist.ForwardRequest {
	req := queryReq(store, query, binds)
	if c == nil {
		return req
	}
	req.Session = uint64(c.ConnectionID)
	req.User = c.User
	req.Host = remoteHost(c)
	req.Database = currentDB(h.Handler, c)
	return req
}

func mergeReply(base, extra persist.ForwardReply) persist.ForwardReply {
	if len(base.Fields) == 0 {
		base.Fields = extra.Fields
	}
	base.Rows = append(base.Rows, extra.Rows...)
	base.RowsAffected += extra.RowsAffected
	return base
}

type txMark int

const (
	txNone txMark = iota
	txBegin
	txEnd
)

func txKind(query string) txMark {
	stmt, err := sqlparser.Parse(query)
	if err != nil {
		return txNone
	}
	switch stmt.(type) {
	case *sqlparser.Begin:
		return txBegin
	case *sqlparser.Commit, *sqlparser.Rollback:
		return txEnd
	default:
		return txNone
	}
}
