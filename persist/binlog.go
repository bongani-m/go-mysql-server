package persist

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
)

var binlogMagic = []byte{0xfe, 0x62, 0x69, 0x6e}

// binlog is the MySQL row-binlog export of committed Raft batches. It is
// written on every node from the same log entry, so a new leader can stream
// it. It is not the consensus log.
type binlog struct {
	mu       chan struct{}
	file     *os.File
	path     string
	name     string
	format   mysql.BinlogFormat
	sid      mysql.SID
	events   []mysql.BinlogEvent
	executed mysql.Mysql56GTIDSet
	position uint32
	notify   chan struct{}
	replicas []registeredReplica
}

type registeredReplica struct {
	host string
	port uint16
}

func openBinlog(dir, serverUUID string) (*binlog, error) {
	sid, err := mysql.ParseSID(serverUUID)
	if err != nil {
		return nil, fmt.Errorf("persist: binlog server uuid: %w", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "binlog.000001")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	b := &binlog{
		mu:       make(chan struct{}, 1),
		file:     file,
		path:     path,
		name:     "binlog.000001",
		format:   mysql.NewMySQL56BinlogFormat(),
		sid:      sid,
		executed: mysql.Mysql56GTIDSet{},
		notify:   make(chan struct{}, 1),
	}
	b.mu <- struct{}{}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}
	if info.Size() == 0 {
		if _, err := file.Write(binlogMagic); err != nil {
			file.Close()
			return nil, err
		}
		b.position = uint32(len(binlogMagic))
		if err := b.writeBootstrap(); err != nil {
			file.Close()
			return nil, err
		}
		return b, nil
	}
	raw, err := io.ReadAll(file)
	if err != nil {
		file.Close()
		return nil, err
	}
	events, format, err := parseBinlog(raw)
	if err != nil {
		file.Close()
		return nil, err
	}
	b.events = events
	if format.FormatVersion != 0 {
		b.format = format
	}
	b.position = uint32(len(raw))
	for _, ev := range events {
		if !ev.IsGTID() {
			continue
		}
		gtid, _, err := ev.GTID(b.format)
		if err != nil {
			file.Close()
			return nil, err
		}
		b.executed = b.executed.AddGTID(gtid).(mysql.Mysql56GTIDSet)
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		file.Close()
		return nil, err
	}
	return b, nil
}

func (b *binlog) lock() {
	<-b.mu
}

func (b *binlog) unlock() {
	b.mu <- struct{}{}
}

func (b *binlog) close() {
	b.lock()
	defer b.unlock()
	if b.file != nil {
		b.file.Close()
		b.file = nil
	}
}

func (b *binlog) writeBootstrap() error {
	meta := mysql.BinlogEventMetadata{ServerID: 1}
	if err := b.writeEventLocked(mysql.NewFormatDescriptionEvent(b.format, meta)); err != nil {
		return err
	}
	return b.writeEventLocked(mysql.NewPreviousGtidsEvent(b.format, meta, mysql.Mysql56GTIDSet{}))
}

func (b *binlog) writeEventLocked(ev mysql.BinlogEvent) error {
	raw := ev.Bytes()
	next := b.position + uint32(len(raw))
	if len(raw) >= 17 {
		binary.LittleEndian.PutUint32(raw[13:17], next)
		mysql.UpdateChecksum(b.format, ev)
	}
	if _, err := b.file.Write(ev.Bytes()); err != nil {
		return err
	}
	b.position = next
	b.events = append(b.events, ev)
	return nil
}

func (b *binlog) signal() {
	select {
	case b.notify <- struct{}{}:
	default:
	}
}

func (s *Store) appendBinlog(index uint64, batch replBatch) error {
	if s.bin == nil {
		return nil
	}
	return s.bin.append(index, batch)
}

func (b *binlog) append(index uint64, batch replBatch) error {
	events, err := b.build(index, batch)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}
	b.lock()
	defer b.unlock()
	for _, ev := range events {
		if err := b.writeEventLocked(ev); err != nil {
			return err
		}
		if ev.IsGTID() {
			gtid, _, err := ev.GTID(b.format)
			if err != nil {
				return err
			}
			b.executed = b.executed.AddGTID(gtid).(mysql.Mysql56GTIDSet)
		}
	}
	b.signal()
	return nil
}

func (b *binlog) build(index uint64, batch replBatch) ([]mysql.BinlogEvent, error) {
	if len(batch.Rows) == 0 && batch.Statement == "" {
		return nil, nil
	}
	meta := mysql.BinlogEventMetadata{ServerID: 1, Timestamp: batch.Unix}
	gtid := mysql.Mysql56GTID{Server: b.sid, Sequence: int64(index)}
	events := []mysql.BinlogEvent{
		mysql.NewMySQLGTIDEvent(b.format, meta, gtid, len(batch.Rows) > 0),
	}
	if len(batch.Rows) == 0 {
		events = append(events, mysql.NewQueryEvent(b.format, meta, mysql.Query{SQL: batch.Statement}))
		return events, nil
	}
	database := batch.Rows[0].Database
	events = append(events, mysql.NewQueryEvent(b.format, meta, mysql.Query{
		Database: database,
		SQL:      "BEGIN",
	}))
	var tableID uint64
	for _, change := range batch.Rows {
		tableID++
		rowEvents, err := b.rowEvents(tableID, change, meta)
		if err != nil {
			return nil, err
		}
		events = append(events, rowEvents...)
	}
	events = append(events, mysql.NewXIDEvent(b.format, meta))
	return events, nil
}

func (b *binlog) rowEvents(tableID uint64, change rowChange, meta mysql.BinlogEventMetadata) ([]mysql.BinlogEvent, error) {
	tableMeta, err := decodeSchema(change.Schema, change.Database, change.Table)
	if err != nil {
		return nil, err
	}
	tableMap, err := tableMapFor(change.Database, change.Table, tableMeta.schema)
	if err != nil {
		return nil, err
	}
	mapEvent, err := mysql.NewTableMapEvent(b.format, meta, tableID, tableMap)
	if err != nil {
		return nil, err
	}
	n := len(tableMeta.schema)
	switch change.Op {
	case int(opInsert):
		data, nulls, err := encodeBinlogRow(tableMeta.schema, change.After)
		if err != nil {
			return nil, err
		}
		rows := mysql.Rows{
			DataColumns: presentColumns(n),
			Rows:        []mysql.Row{{NullColumns: nulls, Data: data}},
		}
		return []mysql.BinlogEvent{mapEvent, mysql.NewWriteRowsEvent(b.format, meta, tableID, rows)}, nil
	case int(opDelete):
		data, nulls, err := encodeBinlogRow(tableMeta.schema, change.Before)
		if err != nil {
			return nil, err
		}
		rows := mysql.Rows{
			IdentifyColumns: presentColumns(n),
			Rows:            []mysql.Row{{NullIdentifyColumns: nulls, Identify: data}},
		}
		return []mysql.BinlogEvent{mapEvent, mysql.NewDeleteRowsEvent(b.format, meta, tableID, rows)}, nil
	case int(opUpdate):
		before, beforeNulls, err := encodeBinlogRow(tableMeta.schema, change.Before)
		if err != nil {
			return nil, err
		}
		after, afterNulls, err := encodeBinlogRow(tableMeta.schema, change.After)
		if err != nil {
			return nil, err
		}
		rows := mysql.Rows{
			IdentifyColumns: presentColumns(n),
			DataColumns:     presentColumns(n),
			Rows: []mysql.Row{{
				NullIdentifyColumns: beforeNulls,
				Identify:            before,
				NullColumns:         afterNulls,
				Data:                after,
			}},
		}
		return []mysql.BinlogEvent{mapEvent, mysql.NewUpdateRowsEvent(b.format, meta, tableID, rows)}, nil
	default:
		return nil, fmt.Errorf("persist: binlog row op %d", change.Op)
	}
}

func presentColumns(n int) mysql.Bitmap {
	bits := mysql.NewServerBitmap(n)
	for i := 0; i < n; i++ {
		bits.Set(i, true)
	}
	return bits
}

func tableMapFor(database, table string, schema sql.Schema) (*mysql.TableMap, error) {
	types := make([]byte, len(schema))
	metadata := make([]uint16, len(schema))
	nulls := mysql.NewServerBitmap(len(schema))
	for i, col := range schema {
		typ, meta := columnBinlogMeta(col)
		types[i] = typ
		metadata[i] = meta
		if col.Nullable {
			nulls.Set(i, true)
		}
	}
	return &mysql.TableMap{
		Database:  database,
		Name:      table,
		Types:     types,
		CanBeNull: nulls,
		Metadata:  metadata,
	}, nil
}

func columnBinlogMeta(col *sql.Column) (byte, uint16) {
	switch col.Type.Type() {
	case query.Type_INT8, query.Type_UINT8:
		return mysql.TypeTiny, 0
	case query.Type_INT16, query.Type_UINT16:
		return mysql.TypeShort, 0
	case query.Type_INT24, query.Type_UINT24:
		return mysql.TypeInt24, 0
	case query.Type_INT32, query.Type_UINT32:
		return mysql.TypeLong, 0
	case query.Type_INT64, query.Type_UINT64:
		return mysql.TypeLongLong, 0
	case query.Type_FLOAT32:
		return mysql.TypeFloat, 4
	case query.Type_FLOAT64:
		return mysql.TypeDouble, 8
	case query.Type_VARCHAR, query.Type_VARBINARY, query.Type_CHAR, query.Type_BINARY, query.Type_TEXT, query.Type_BLOB:
		max := uint16(255)
		if st, ok := col.Type.(sql.StringType); ok && st.MaxByteLength() > 0 && st.MaxByteLength() <= 65535 {
			max = uint16(st.MaxByteLength())
		}
		if col.Type.Type() == query.Type_TEXT || col.Type.Type() == query.Type_BLOB || max > 255 {
			n := uint16(1)
			if max > 255 {
				n = 2
			}
			if max > 65535 {
				n = 4
			}
			return mysql.TypeBlob, n
		}
		return mysql.TypeVarchar, max
	default:
		return mysql.TypeBlob, 2
	}
}

func encodeBinlogRow(schema sql.Schema, raw []byte) ([]byte, mysql.Bitmap, error) {
	nulls := mysql.NewServerBitmap(len(schema))
	if len(raw) == 0 {
		for i := range schema {
			nulls.Set(i, true)
		}
		return nil, nulls, nil
	}
	row, err := decodeRow(context.Background(), schema, raw)
	if err != nil {
		return nil, mysql.Bitmap{}, err
	}
	var data []byte
	for i, col := range schema {
		if i >= len(row) || row[i] == nil {
			nulls.Set(i, true)
			continue
		}
		encoded, err := encodeBinlogValue(col, row[i])
		if err != nil {
			return nil, mysql.Bitmap{}, err
		}
		data = append(data, encoded...)
	}
	return data, nulls, nil
}

func encodeBinlogValue(col *sql.Column, value interface{}) ([]byte, error) {
	converted, _, err := col.Type.Convert(context.Background(), value)
	if err != nil {
		converted = value
	}
	if converted == nil {
		return nil, nil
	}
	switch col.Type.Type() {
	case query.Type_INT8, query.Type_UINT8:
		n, ok := asUint64(converted)
		if !ok {
			break
		}
		return []byte{byte(n)}, nil
	case query.Type_INT16, query.Type_UINT16:
		n, ok := asUint64(converted)
		if !ok {
			break
		}
		var buf [2]byte
		binary.LittleEndian.PutUint16(buf[:], uint16(n))
		return buf[:], nil
	case query.Type_INT24, query.Type_UINT24:
		n, ok := asUint64(converted)
		if !ok {
			break
		}
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], uint32(n))
		return buf[:3], nil
	case query.Type_INT32, query.Type_UINT32:
		n, ok := asUint64(converted)
		if !ok {
			break
		}
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], uint32(n))
		return buf[:], nil
	case query.Type_INT64, query.Type_UINT64:
		n, ok := asUint64(converted)
		if !ok {
			break
		}
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], n)
		return buf[:], nil
	case query.Type_FLOAT32:
		f, ok := converted.(float32)
		if !ok {
			break
		}
		var buf [4]byte
		binary.LittleEndian.PutUint32(buf[:], math.Float32bits(f))
		return buf[:], nil
	case query.Type_FLOAT64:
		f, ok := converted.(float64)
		if !ok {
			break
		}
		var buf [8]byte
		binary.LittleEndian.PutUint64(buf[:], math.Float64bits(f))
		return buf[:], nil
	}
	text := fmt.Sprint(converted)
	if s, ok := converted.(string); ok {
		text = s
	} else if b, ok := converted.([]byte); ok {
		text = string(b)
	}
	return encodeLenPrefixed([]byte(text), columnLengthBytes(col)), nil
}

func columnLengthBytes(col *sql.Column) int {
	_, meta := columnBinlogMeta(col)
	switch col.Type.Type() {
	case query.Type_VARCHAR, query.Type_VARBINARY:
		if meta > 255 {
			return 2
		}
		return 1
	case query.Type_TEXT, query.Type_BLOB:
		if meta == 0 {
			return 1
		}
		return int(meta)
	default:
		typ, _ := columnBinlogMeta(col)
		if typ == mysql.TypeBlob {
			if meta == 0 {
				return 2
			}
			return int(meta)
		}
		if typ == mysql.TypeVarchar && meta > 255 {
			return 2
		}
		return 1
	}
}

func encodeLenPrefixed(b []byte, n int) []byte {
	if n < 1 {
		n = 1
	}
	buf := make([]byte, n+len(b))
	switch n {
	case 1:
		buf[0] = byte(len(b))
	case 2:
		binary.LittleEndian.PutUint16(buf, uint16(len(b)))
	case 3:
		var tmp [4]byte
		binary.LittleEndian.PutUint32(tmp[:], uint32(len(b)))
		copy(buf, tmp[:3])
	default:
		binary.LittleEndian.PutUint32(buf, uint32(len(b)))
	}
	copy(buf[n:], b)
	return buf
}

func asUint64(v interface{}) (uint64, bool) {
	switch n := v.(type) {
	case int:
		return uint64(n), true
	case int8:
		return uint64(n), true
	case int16:
		return uint64(n), true
	case int32:
		return uint64(n), true
	case int64:
		return uint64(n), true
	case uint:
		return uint64(n), true
	case uint8:
		return uint64(n), true
	case uint16:
		return uint64(n), true
	case uint32:
		return uint64(n), true
	case uint64:
		return n, true
	default:
		return 0, false
	}
}

func (b *binlog) read() ([]mysql.BinlogEvent, mysql.BinlogFormat, error) {
	b.lock()
	defer b.unlock()
	raw, err := os.ReadFile(b.path)
	if err != nil {
		return nil, mysql.BinlogFormat{}, err
	}
	return parseBinlog(raw)
}

func parseBinlog(raw []byte) ([]mysql.BinlogEvent, mysql.BinlogFormat, error) {
	if len(raw) < len(binlogMagic) {
		return nil, mysql.BinlogFormat{}, io.ErrUnexpectedEOF
	}
	rest := raw[len(binlogMagic):]
	var format mysql.BinlogFormat
	var events []mysql.BinlogEvent
	for len(rest) > 0 {
		if len(rest) < 19 {
			return events, format, fmt.Errorf("persist: truncated binlog")
		}
		n := int(binary.LittleEndian.Uint32(rest[9:13]))
		if n < 19 || n > len(rest) {
			return events, format, fmt.Errorf("persist: bad binlog event length %d", n)
		}
		ev := mysql.NewMysql56BinlogEvent(rest[:n])
		if !ev.IsValid() {
			return events, format, fmt.Errorf("persist: invalid binlog event")
		}
		if ev.IsFormatDescription() {
			parsed, err := ev.Format()
			if err != nil {
				return nil, mysql.BinlogFormat{}, err
			}
			format = parsed
		}
		events = append(events, ev)
		rest = rest[n:]
	}
	return events, format, nil
}

// ReadBinlog parses the on-disk binlog for this node.
func (s *Store) ReadBinlog() ([]mysql.BinlogEvent, mysql.BinlogFormat, error) {
	if s.bin == nil {
		return nil, mysql.BinlogFormat{}, fmt.Errorf("persist: binlog is not enabled")
	}
	return s.bin.read()
}

var _ binlogreplication.BinlogPrimaryController = (*Store)(nil)

func (s *Store) binlogOrErr() (*binlog, error) {
	if s.bin == nil {
		return nil, fmt.Errorf("persist: binlog replication requires cluster mode")
	}
	return s.bin, nil
}

// RegisterReplica implements binlogreplication.BinlogPrimaryController.
func (s *Store) RegisterReplica(_ *sql.Context, _ *mysql.Conn, host string, port uint16) error {
	b, err := s.binlogOrErr()
	if err != nil {
		return err
	}
	b.lock()
	defer b.unlock()
	b.replicas = append(b.replicas, registeredReplica{host: host, port: port})
	return nil
}

// BinlogDumpGtid streams row events starting after the replica's executed set.
// It returns when the connection write fails.
func (s *Store) BinlogDumpGtid(ctx *sql.Context, conn *mysql.Conn, executed mysql.GTIDSet) error {
	b, err := s.binlogOrErr()
	if err != nil {
		return mysql.NewSQLError(mysql.ERMasterFatalReadingBinlog, "HY000", "%v", err)
	}
	sent := 0
	for {
		if ctx != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		b.lock()
		events := append([]mysql.BinlogEvent(nil), b.events...)
		format := b.format
		b.unlock()
		skip := false
		for _, ev := range events[sent:] {
			if ev.IsGTID() {
				gtid, _, err := ev.GTID(format)
				if err != nil {
					return err
				}
				skip = executed != nil && executed.ContainsGTID(gtid)
			}
			if skip && !ev.IsFormatDescription() && !ev.IsPreviousGTIDs() {
				sent++
				continue
			}
			if err := conn.WriteBinlogEvent(ev, false); err != nil {
				return err
			}
			sent++
		}
		select {
		case <-b.notify:
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// ListReplicas implements binlogreplication.BinlogPrimaryController.
func (s *Store) ListReplicas(*sql.Context) error {
	return nil
}

// ListBinaryLogs implements binlogreplication.BinlogPrimaryController.
func (s *Store) ListBinaryLogs(*sql.Context) ([]binlogreplication.BinaryLogFileMetadata, error) {
	b, err := s.binlogOrErr()
	if err != nil {
		return nil, nil
	}
	b.lock()
	defer b.unlock()
	info, err := os.Stat(b.path)
	if err != nil {
		return nil, err
	}
	return []binlogreplication.BinaryLogFileMetadata{{
		Name: b.name,
		Size: uint64(info.Size()),
	}}, nil
}

// GetBinaryLogStatus implements binlogreplication.BinlogPrimaryController.
func (s *Store) GetBinaryLogStatus(*sql.Context) ([]binlogreplication.BinaryLogStatus, error) {
	b, err := s.binlogOrErr()
	if err != nil {
		return nil, nil
	}
	b.lock()
	defer b.unlock()
	return []binlogreplication.BinaryLogStatus{{
		File:          b.name,
		Position:      uint(b.position),
		ExecutedGtids: b.executed.String(),
	}}, nil
}
