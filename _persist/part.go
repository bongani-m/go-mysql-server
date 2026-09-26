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
}

func newPartHandler(inner *forwardHandler, meta *persist.Store, cfg *partConfig) mysql.Handler {
	return &partHandler{
		forwardHandler: inner,
		meta:           meta,
		cfg:            cfg,
		pins:           make(map[uint32]*partPin),
	}
}

func (h *partHandler) ConnectionClosed(c *mysql.Conn) {
	h.mu.Lock()
	delete(h.pins, c.ConnectionID)
	h.mu.Unlock()
	h.forwardHandler.ConnectionClosed(c)
}

func (h *partHandler) ComResetConnection(c *mysql.Conn) error {
	h.mu.Lock()
	delete(h.pins, c.ConnectionID)
	h.mu.Unlock()
	return h.forwardHandler.ComResetConnection(c)
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
		if err := h.savePlacement(route.Place); err != nil {
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
	return h.meta.Placements()
}

func (h *partHandler) savePlacement(p persist.TablePlacement) error {
	if h.meta.IsLeader() {
		return h.meta.SavePlacement(p)
	}
	reply, err := dialStore(h.meta, "", queryReq(h.meta, placementSQL(p), nil))
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
	reply, err := dialStore(h.meta, "", h.request(c, h.meta, query, nil))
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
	addr, err := h.leaderForward(h.cfg.Contacts[shard])
	if err != nil {
		return persist.ForwardReply{}, err
	}
	reply, err := dialStore(h.store, addr, h.request(c, h.store, query, binds))
	if err != nil {
		return persist.ForwardReply{}, err
	}
	if reply.Err != "" {
		return persist.ForwardReply{}, mysql.NewSQLError(mysql.ERUnknownError, "HY000", "%s", reply.Err)
	}
	return reply, nil
}

func (h *partHandler) scatter(c *mysql.Conn, query string, binds []persist.ForwardBind) (persist.ForwardReply, error) {
	var merged persist.ForwardReply
	req := h.request(c, h.store, query, binds)
	for shard := 0; shard < h.cfg.Count; shard++ {
		reply, err := dialStore(h.store, h.cfg.Contacts[shard], req)
		if err != nil {
			return persist.ForwardReply{}, err
		}
		if reply.Err != "" {
			return persist.ForwardReply{}, mysql.NewSQLError(mysql.ERUnknownError, "HY000", "%s", reply.Err)
		}
		merged = mergeReply(merged, reply)
	}
	return merged, nil
}

func (h *partHandler) rejectDups(c *mysql.Conn, route Route) error {
	for _, check := range route.Checks {
		literal := check.Text
		if check.Quote {
			literal = "'" + strings.ReplaceAll(check.Text, "'", "''") + "'"
		}
		q := fmt.Sprintf("SELECT 1 FROM %s.%s WHERE %s = %s LIMIT 1", route.Place.DB, route.Place.Table, check.Column, literal)
		reply, err := h.scatter(c, q, nil)
		if err != nil {
			return err
		}
		if len(reply.Rows) > 0 {
			return mysql.NewSQLError(mysql.ERDupEntry, "23000", "Duplicate entry '%s' for key '%s'", check.Text, check.Column)
		}
	}
	return nil
}

// leaderForward resolves the leader's forward address from one member of the group.
func (h *partHandler) leaderForward(contact string) (string, error) {
	reply, err := dialStore(h.store, contact, queryReq(h.store, "SHOW RAFT STATUS", nil))
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

func dialStore(store *persist.Store, addr string, req persist.ForwardRequest) (persist.ForwardReply, error) {
	var client *persist.ForwardClient
	var err error
	if addr == "" {
		client, err = store.DialForward()
	} else {
		client, err = store.DialForwardAddr(addr)
	}
	if err != nil {
		return persist.ForwardReply{}, err
	}
	defer client.Close(req.Node, req.Session)
	return client.Exec(req, store.ApplyTimeout())
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
