package persist

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/apd/v3"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// primaryKey is the memcomparable row key for a primary key. Parts are
// concatenated. Each part is self-delimiting, so a shorter string sorts before
// a longer one instead of by its length.
func primaryKey(ctx context.Context, schema sql.Schema, ordinals []int, row sql.Row) ([]byte, error) {
	var buf []byte
	for _, ord := range ordinals {
		if ord < 0 || ord >= len(row) {
			return nil, fmt.Errorf("persist: primary key ordinal %d is outside the row", ord)
		}
		var typ sql.Type
		if ord < len(schema) {
			typ = schema[ord].Type
		}
		part, err := encodeField(ctx, indexField{typ: typ}, row[ord])
		if err != nil {
			return nil, err
		}
		buf = append(buf, part...)
	}
	return buf, nil
}

type indexField struct {
	ordinal int
	typ     sql.Type
	desc    bool
	prefix  uint16
}

func indexFields(schema sql.Schema, idx storedIndex) ([]indexField, error) {
	fields := make([]indexField, len(idx.Columns))
	for i, name := range idx.Columns {
		ord := columnOrdinal(schema, name)
		if ord < 0 {
			return nil, fmt.Errorf("persist: index %s column %s is not in the schema", idx.Name, name)
		}
		field := indexField{ordinal: ord, typ: schema[ord].Type}
		if i < len(idx.Lengths) {
			field.prefix = idx.Lengths[i]
		}
		if i < len(idx.Descending) {
			field.desc = idx.Descending[i]
		}
		fields[i] = field
	}
	return fields, nil
}

func pkFields(schema sql.Schema, ordinals []int) []indexField {
	fields := make([]indexField, len(ordinals))
	for i, ord := range ordinals {
		fields[i] = indexField{ordinal: ord, typ: schema[ord].Type}
	}
	return fields
}

// encodeField encodes one index or primary-key part. prefix truncates strings
// the way a prefix index does. desc inverts the bytes so the part sorts descending.
func encodeField(ctx context.Context, field indexField, value interface{}) ([]byte, error) {
	if value != nil && field.typ != nil {
		if converted, _, err := field.typ.Convert(ctx, value); err == nil {
			value = converted
		}
	}
	if field.prefix > 0 {
		value = prefixValue(value, field.prefix)
	}
	return encodeKeyPart(value, field.desc), nil
}

func encodeKeyPart(value interface{}, descending bool) []byte {
	part := encodeAscending(value)
	if descending {
		for i, b := range part {
			part[i] = ^b
		}
	}
	return part
}

func encodeAscending(value interface{}) []byte {
	if value == nil {
		return []byte{0x00}
	}
	switch v := value.(type) {
	case string:
		return encodeTextKey([]byte(v))
	case []byte:
		return encodeTextKey(v)
	case bool:
		if v {
			return encodeSigned(1)
		}
		return encodeSigned(0)
	case int:
		return encodeSigned(int64(v))
	case int8:
		return encodeSigned(int64(v))
	case int16:
		return encodeSigned(int64(v))
	case int32:
		return encodeSigned(int64(v))
	case int64:
		return encodeSigned(v)
	case uint:
		return encodeUnsigned(uint64(v))
	case uint8:
		return encodeUnsigned(uint64(v))
	case uint16:
		return encodeUnsigned(uint64(v))
	case uint32:
		return encodeUnsigned(uint64(v))
	case uint64:
		return encodeUnsigned(v)
	case float32:
		return encodeFloat(float64(v))
	case float64:
		return encodeFloat(v)
	case time.Time:
		return encodeTimeKey(v)
	case *apd.Decimal:
		return encodeDecimalKey(v)
	case apd.Decimal:
		return encodeDecimalKey(&v)
	case types.Timespan:
		return encodeSigned(int64(v))
	case types.GeometryValue:
		return encodeTextKey(v.Serialize())
	default:
		return encodeTextKey([]byte(fmt.Sprint(v)))
	}
}

func encodeSigned(v int64) []byte {
	var buf [9]byte
	buf[0] = 0x01
	binary.BigEndian.PutUint64(buf[1:], uint64(v)^(1<<63))
	return buf[:]
}

func encodeUnsigned(v uint64) []byte {
	var buf [9]byte
	buf[0] = 0x01
	binary.BigEndian.PutUint64(buf[1:], v)
	return buf[:]
}

func encodeFloat(v float64) []byte {
	if v == 0 {
		v = 0
	}
	bits := math.Float64bits(v)
	if bits&(1<<63) != 0 {
		bits = ^bits
	} else {
		bits ^= 1 << 63
	}
	var buf [9]byte
	buf[0] = 0x01
	binary.BigEndian.PutUint64(buf[1:], bits)
	return buf[:]
}

// encodeTextKey terminates the bytes so "aa" sorts before "b". A leading
// length would sort by size. A zero byte is escaped so the terminator is unique.
func encodeTextKey(b []byte) []byte {
	buf := make([]byte, 0, len(b)+4)
	buf = append(buf, 0x01)
	for _, c := range b {
		if c == 0x00 {
			buf = append(buf, 0x00, 0xFF)
			continue
		}
		buf = append(buf, c)
	}
	return append(buf, 0x00, 0x00)
}

func encodeTimeKey(v time.Time) []byte {
	u := v.UTC()
	var buf [1 + 4 + 5 + 4]byte
	buf[0] = 0x01
	binary.BigEndian.PutUint32(buf[1:5], uint32(int32(u.Year()))^(1<<31))
	buf[5] = byte(u.Month())
	buf[6] = byte(u.Day())
	buf[7] = byte(u.Hour())
	buf[8] = byte(u.Minute())
	buf[9] = byte(u.Second())
	binary.BigEndian.PutUint32(buf[10:], uint32(u.Nanosecond()))
	return buf[:]
}

// encodeDecimalKey orders -10 < -2 < 0 < 2 < 10, and encodes 1.0 the same as 1.
func encodeDecimalKey(d *apd.Decimal) []byte {
	if d == nil || d.Form != apd.Finite {
		if d != nil && d.Form == apd.Infinite {
			if d.Negative {
				return []byte{0x01, 0x00}
			}
			return []byte{0x01, 0xFE}
		}
		return []byte{0x01, 0xFF}
	}
	n := new(apd.Decimal)
	n.Reduce(d)
	if n.IsZero() {
		return []byte{0x01, 0x80}
	}
	neg := n.Negative
	n.Negative = false
	text := n.Coeff.Text(10)
	msdExp := int64(n.Exponent) + int64(len(text)) - 1
	body := encodePositiveDecimal(msdExp, text)
	if neg {
		for i := range body {
			body[i] = ^body[i]
		}
	}
	out := make([]byte, 1+len(body))
	out[0] = 0x01
	copy(out[1:], body)
	return out
}

func encodePositiveDecimal(msdExp int64, text string) []byte {
	var expb [4]byte
	binary.BigEndian.PutUint32(expb[:], uint32(int32(msdExp))^(1<<31))
	buf := make([]byte, 0, 1+4+len(text)+1)
	buf = append(buf, 0x81)
	buf = append(buf, expb[:]...)
	buf = append(buf, text...)
	return append(buf, 0x00)
}

// prefixEnd is the smallest key strictly after every key with this prefix.
// A nil result means the prefix is the maximum possible key.
func prefixEnd(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

func indexEntryKey(colKey, rowKey []byte, unique, hasNull bool) []byte {
	if unique && !hasNull {
		return append([]byte(nil), colKey...)
	}
	out := make([]byte, 0, len(colKey)+len(rowKey))
	out = append(out, colKey...)
	out = append(out, rowKey...)
	return out
}

func encodeIndexColumns(ctx context.Context, fields []indexField, row sql.Row) (colKey []byte, hasNull bool, err error) {
	var buf []byte
	for _, field := range fields {
		if field.ordinal < 0 || field.ordinal >= len(row) {
			return nil, false, fmt.Errorf("persist: index ordinal %d is outside the row", field.ordinal)
		}
		if row[field.ordinal] == nil {
			hasNull = true
		}
		part, err := encodeField(ctx, field, row[field.ordinal])
		if err != nil {
			return nil, false, err
		}
		buf = append(buf, part...)
	}
	return buf, hasNull, nil
}
