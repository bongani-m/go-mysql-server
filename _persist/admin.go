package main

import (
	"fmt"
	"strconv"
	"strings"

	querypb "github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/go-mysql-server/persist"
)

const (
	adminStatus = "status"
	adminAdd    = "add"
	adminRemove = "remove"
)

type adminCmd struct {
	kind string
	id   string
	addr string
}

// parseAdmin recognizes SHOW RAFT STATUS, RAFT ADD VOTER, and RAFT REMOVE SERVER.
// The SQL parser does not know these statements.
func parseAdmin(query string) (adminCmd, bool) {
	q := strings.TrimSpace(query)
	if strings.HasSuffix(q, ";") {
		q = strings.TrimSpace(strings.TrimSuffix(q, ";"))
	}
	if strings.EqualFold(q, "SHOW RAFT STATUS") {
		return adminCmd{kind: adminStatus}, true
	}
	fields, err := splitAdmin(q)
	if err != nil || len(fields) == 0 {
		return adminCmd{}, false
	}
	if len(fields) == 5 && strings.EqualFold(fields[0], "RAFT") && strings.EqualFold(fields[1], "ADD") && strings.EqualFold(fields[2], "VOTER") {
		if fields[3] == "" || fields[4] == "" {
			return adminCmd{}, false
		}
		return adminCmd{kind: adminAdd, id: fields[3], addr: fields[4]}, true
	}
	if len(fields) == 4 && strings.EqualFold(fields[0], "RAFT") && strings.EqualFold(fields[1], "REMOVE") && strings.EqualFold(fields[2], "SERVER") {
		if fields[3] == "" {
			return adminCmd{}, false
		}
		return adminCmd{kind: adminRemove, id: fields[3]}, true
	}
	return adminCmd{}, false
}

func splitAdmin(q string) ([]string, error) {
	var out []string
	var b strings.Builder
	var quote byte
	for i := 0; i < len(q); i++ {
		c := q[i]
		if quote != 0 {
			if c == quote {
				quote = 0
				continue
			}
			b.WriteByte(c)
			continue
		}
		if c == '\'' || c == '"' {
			quote = c
			continue
		}
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' {
			if b.Len() > 0 {
				out = append(out, b.String())
				b.Reset()
			}
			continue
		}
		b.WriteByte(c)
	}
	if quote != 0 {
		return nil, fmt.Errorf("unterminated quote")
	}
	if b.Len() > 0 {
		out = append(out, b.String())
	}
	return out, nil
}

func runAdmin(store *persist.Store, cmd adminCmd) (persist.ForwardReply, error) {
	switch cmd.kind {
	case adminStatus:
		return statusReply(store), nil
	case adminAdd:
		if err := store.AddVoter(cmd.id, cmd.addr); err != nil {
			return persist.ForwardReply{}, err
		}
		return persist.ForwardReply{Info: "voter added"}, nil
	case adminRemove:
		if err := store.RemoveServer(cmd.id); err != nil {
			return persist.ForwardReply{}, err
		}
		return persist.ForwardReply{Info: "server removed"}, nil
	default:
		return persist.ForwardReply{}, fmt.Errorf("persist: unknown statement")
	}
}

func statusReply(store *persist.Store) persist.ForwardReply {
	st := store.Status()
	names := []string{"role", "leader", "commit_index", "applied_index", "lag"}
	vals := []string{
		st.Role,
		st.Leader,
		strconv.FormatUint(st.Commit, 10),
		strconv.FormatUint(st.Applied, 10),
		strconv.FormatUint(st.Lag, 10),
	}
	fields := make([]persist.ForwardField, len(names))
	cells := make([]persist.ForwardCell, len(names))
	for i, name := range names {
		fields[i] = persist.ForwardField{
			Name:         name,
			Type:         int32(querypb.Type_VARCHAR),
			Charset:      45,
			ColumnLength: 256,
		}
		cells[i] = persist.ForwardCell{Type: int32(querypb.Type_VARCHAR), Raw: []byte(vals[i])}
	}
	return persist.ForwardReply{Fields: fields, Rows: [][]persist.ForwardCell{cells}}
}
