// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sqltypes implements interfaces and types that represent SQL values.
package sqltypes

import (
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

var (
	// NULL represents the NULL value.
	NULL = Value{}
	// DontEscape tells you if a character should not be escaped.
	DontEscape = byte(255)
	nullstr    = []byte("null")
)

// BinWriter interface is used for encoding values.
// Types like bytes.Buffer conform to this interface.
// We expect the writer objects to be in-memory buffers.
// So, we don't expect the write operations to fail.
type BinWriter interface {
	Write([]byte) (int, error)
	WriteByte(byte) error
}

// Value can store any SQL value. If the value represents
// an integral type, the bytes are always stored as a canonical
// representation that matches how MySQL returns such values.
type Value struct {
	typ querypb.Type
	val []byte
}

// NewValue builds a Value using typ and val. If the value and typ
// don't match, it returns an error.
func NewValue(typ querypb.Type, val []byte) (v Value, err error) {
	switch {
	case IsSigned(typ):
		if _, err := strconv.ParseInt(string(val), 0, 64); err != nil {
			return NULL, err
		}
		return MakeTrusted(typ, val), nil
	case IsUnsigned(typ):
		if _, err := strconv.ParseUint(string(val), 0, 64); err != nil {
			return NULL, err
		}
		return MakeTrusted(typ, val), nil
	case IsFloat(typ) || typ == Decimal:
		if _, err := strconv.ParseFloat(string(val), 64); err != nil {
			return NULL, err
		}
		return MakeTrusted(typ, val), nil
	case IsQuoted(typ) || typ == Bit || typ == Null:
		return MakeTrusted(typ, val), nil
	}
	// All other types are unsafe or invalid.
	return NULL, fmt.Errorf("invalid type specified for MakeValue: %v", typ)
}

// MakeTrusted makes a new Value based on the type.
// If the value is an integral, then val must be in its canonical
// form. This function should only be used if you know the value
// and type conform to the rules.  Every place this function is
// called, a comment is needed that explains why it's justified.
// Functions within this package are exempt.
func MakeTrusted(typ querypb.Type, val []byte) Value {
	if typ == Null {
		return NULL
	}
	return Value{typ: typ, val: val}
}

// NewInt64 builds an Int64 Value.
func NewInt64(v int64) Value {
	return MakeTrusted(Int64, strconv.AppendInt(nil, v, 10))
}

// NewInt32 builds an Int64 Value.
func NewInt32(v int32) Value {
	return MakeTrusted(Int32, strconv.AppendInt(nil, int64(v), 10))
}

// NewUint64 builds an Uint64 Value.
func NewUint64(v uint64) Value {
	return MakeTrusted(Uint64, strconv.AppendUint(nil, v, 10))
}

// NewFloat32 builds an Float64 Value.
func NewFloat32(v float32) Value {
	return MakeTrusted(Float32, strconv.AppendFloat(nil, float64(v), 'f', -1, 64))
}

// NewFloat64 builds an Float64 Value.
func NewFloat64(v float64) Value {
	return MakeTrusted(Float64, strconv.AppendFloat(nil, v, 'g', -1, 64))
}

// NewVarChar builds a VarChar Value.
func NewVarChar(v string) Value {
	return MakeTrusted(VarChar, []byte(v))
}

// NewVarBinary builds a VarBinary Value.
// The input is a string because it's the most common use case.
func NewVarBinary(v string) Value {
	return MakeTrusted(VarBinary, []byte(v))
}

// NewIntegral builds an integral type from a string representation.
// The type will be Int64 or Uint64. Int64 will be preferred where possible.
func NewIntegral(val string) (n Value, err error) {
	signed, err := strconv.ParseInt(val, 0, 64)
	if err == nil {
		return MakeTrusted(Int64, strconv.AppendInt(nil, signed, 10)), nil
	}
	unsigned, err := strconv.ParseUint(val, 0, 64)
	if err != nil {
		return Value{}, err
	}
	return MakeTrusted(Uint64, strconv.AppendUint(nil, unsigned, 10)), nil
}

// MakeString makes a VarBinary Value.
func MakeString(val []byte) Value {
	return MakeTrusted(VarBinary, val)
}

// BuildValue builds a value from any go type. sqltype.Value is
// also allowed.
func BuildValue(goval interface{}) (v Value, err error) {
	// Look for the most common types first.
	switch goval := goval.(type) {
	case nil:
		// no op
	case []byte:
		v = MakeTrusted(VarBinary, goval)
	case int64:
		v = MakeTrusted(Int64, strconv.AppendInt(nil, int64(goval), 10))
	case uint64:
		v = MakeTrusted(Uint64, strconv.AppendUint(nil, uint64(goval), 10))
	case float64:
		v = MakeTrusted(Float64, strconv.AppendFloat(nil, goval, 'f', -1, 64))
	case int:
		v = MakeTrusted(Int64, strconv.AppendInt(nil, int64(goval), 10))
	case int8:
		v = MakeTrusted(Int8, strconv.AppendInt(nil, int64(goval), 10))
	case int16:
		v = MakeTrusted(Int16, strconv.AppendInt(nil, int64(goval), 10))
	case int32:
		v = MakeTrusted(Int32, strconv.AppendInt(nil, int64(goval), 10))
	case uint:
		v = MakeTrusted(Uint64, strconv.AppendUint(nil, uint64(goval), 10))
	case uint8:
		v = MakeTrusted(Uint8, strconv.AppendUint(nil, uint64(goval), 10))
	case uint16:
		v = MakeTrusted(Uint16, strconv.AppendUint(nil, uint64(goval), 10))
	case uint32:
		v = MakeTrusted(Uint32, strconv.AppendUint(nil, uint64(goval), 10))
	case float32:
		v = MakeTrusted(Float32, strconv.AppendFloat(nil, float64(goval), 'f', -1, 64))
	case string:
		v = MakeTrusted(VarBinary, []byte(goval))
	case time.Time:
		v = MakeTrusted(Datetime, []byte(goval.Format("2006-01-02 15:04:05")))
	case Value:
		v = goval
	case *querypb.BindVariable:
		return ValueFromBytes(goval.Type, goval.Value)
	default:
		return v, fmt.Errorf("unexpected type %T: %v", goval, goval)
	}
	return v, nil
}

// BuildConverted is like BuildValue except that it tries to
// convert a string or []byte to an integral if the target type
// is an integral. We don't perform other implicit conversions
// because they're unsafe.
func BuildConverted(typ querypb.Type, goval interface{}) (v Value, err error) {
	if IsIntegral(typ) {
		switch goval := goval.(type) {
		case []byte:
			return ValueFromBytes(typ, goval)
		case string:
			return ValueFromBytes(typ, []byte(goval))
		case Value:
			if goval.IsQuoted() {
				return ValueFromBytes(typ, goval.Raw())
			}
		}
	}
	return BuildValue(goval)
}

// ValueFromBytes builds a Value using typ and val. It ensures that val
// matches the requested type. If type is an integral it's converted to
// a canonical form. Otherwise, the original representation is preserved.
func ValueFromBytes(typ querypb.Type, val []byte) (v Value, err error) {
	switch {
	case IsSigned(typ):
		signed, err := strconv.ParseInt(string(val), 0, 64)
		if err != nil {
			return NULL, err
		}
		v = MakeTrusted(typ, strconv.AppendInt(nil, signed, 10))
	case IsUnsigned(typ):
		unsigned, err := strconv.ParseUint(string(val), 0, 64)
		if err != nil {
			return NULL, err
		}
		v = MakeTrusted(typ, strconv.AppendUint(nil, unsigned, 10))
	case typ == Tuple:
		return NULL, errors.New("tuple not allowed for ValueFromBytes")
	case IsFloat(typ) || typ == Decimal:
		_, err := strconv.ParseFloat(string(val), 64)
		if err != nil {
			return NULL, err
		}
		// After verification, we preserve the original representation.
		fallthrough
	default:
		v = MakeTrusted(typ, val)
	}
	return v, nil
}

// BuildIntegral builds an integral type from a string representation.
// The type will be Int64 or Uint64. Int64 will be preferred where possible.
func BuildIntegral(val string) (n Value, err error) {
	signed, err := strconv.ParseInt(val, 0, 64)
	if err == nil {
		return MakeTrusted(Int64, strconv.AppendInt(nil, signed, 10)), nil
	}
	unsigned, err := strconv.ParseUint(val, 0, 64)
	if err != nil {
		return Value{}, err
	}
	return MakeTrusted(Uint64, strconv.AppendUint(nil, unsigned, 10)), nil
}

// Type returns the type of Value.
func (v Value) Type() querypb.Type {
	return v.typ
}

// Raw returns the raw bytes. All types are currently implemented as []byte.
// You should avoid using this function. If you do, you should treat the
// bytes as read-only.
func (v Value) Raw() []byte {
	return v.val
}

// Len returns the length.
func (v Value) Len() int {
	return len(v.val)
}

// Values represents the array of Value.
type Values []Value

// Len implements the interface.
func (vs Values) Len() int {
	len := 0
	for _, v := range vs {
		len += v.Len()
	}
	return len
}

// String returns the raw value as a string.
func (v Value) String() string {
	return common.BytesToString(v.val)
}

// ToNative converts Value to a native go type.
// This does not work for sqltypes.Tuple. The function
// panics if there are inconsistencies.
func (v Value) ToNative() interface{} {
	var out interface{}
	var err error
	switch {
	case v.typ == Null:
		// no-op
	case IsSigned(v.typ):
		out, err = v.ParseInt64()
	case IsUnsigned(v.typ):
		out, err = v.ParseUint64()
	case IsFloat(v.typ):
		out, err = v.ParseFloat64()
	case v.typ == Tuple:
		err = errors.New("unexpected tuple")
	default:
		out = v.val
	}
	if err != nil {
		panic(err)
	}
	return out
}

// ParseInt64 will parse a Value into an int64. It does
// not check the type.
func (v Value) ParseInt64() (val int64, err error) {
	return strconv.ParseInt(v.String(), 10, 64)
}

// ParseUint64 will parse a Value into a uint64. It does
// not check the type.
func (v Value) ParseUint64() (val uint64, err error) {
	return strconv.ParseUint(v.String(), 10, 64)
}

// ParseFloat64 will parse a Value into an float64. It does
// not check the type.
func (v Value) ParseFloat64() (val float64, err error) {
	return strconv.ParseFloat(v.String(), 64)
}

// EncodeSQL encodes the value into an SQL statement. Can be binary.
func (v Value) EncodeSQL(b BinWriter) {
	// ToNative panics if v is invalid.
	_ = v.ToNative()
	switch {
	case v.typ == Null:
		writebytes(nullstr, b)
	case IsQuoted(v.typ):
		encodeBytesSQL(v.val, b)
	default:
		writebytes(v.val, b)
	}
}

// EncodeASCII encodes the value using 7-bit clean ascii bytes.
func (v Value) EncodeASCII(b BinWriter) {
	// ToNative panics if v is invalid.
	_ = v.ToNative()
	switch {
	case v.typ == Null:
		writebytes(nullstr, b)
	case IsQuoted(v.typ):
		encodeBytesASCII(v.val, b)
	default:
		writebytes(v.val, b)
	}
}

// IsNull returns true if Value is null.
func (v Value) IsNull() bool {
	return v.typ == Null
}

// IsIntegral returns true if Value is an integral.
func (v Value) IsIntegral() bool {
	return IsIntegral(v.typ)
}

// IsSigned returns true if Value is a signed integral.
func (v Value) IsSigned() bool {
	return IsSigned(v.typ)
}

// IsUnsigned returns true if Value is an unsigned integral.
func (v Value) IsUnsigned() bool {
	return IsUnsigned(v.typ)
}

// IsFloat returns true if Value is a float.
func (v Value) IsFloat() bool {
	return IsFloat(v.typ)
}

// IsQuoted returns true if Value must be SQL-quoted.
func (v Value) IsQuoted() bool {
	return IsQuoted(v.typ)
}

// IsText returns true if Value is a collatable text.
func (v Value) IsText() bool {
	return IsText(v.typ)
}

// IsBinary returns true if Value is binary.
func (v Value) IsBinary() bool {
	return IsBinary(v.typ)
}

// IsTemporal returns true if Value is time type.
func (v Value) IsTemporal() bool {
	return IsTemporal(v.typ)
}

// ToString returns the value as MySQL would return it as string.
// If the value is not convertible like in the case of Expression, it returns nil.
func (v Value) ToString() string {
	if v.typ == Expression {
		return ""
	}
	return common.BytesToString(v.val)
}

func writebyte(c byte, b BinWriter) {
	if err := b.WriteByte(c); err != nil {
		panic(err)
	}
}

func writebytes(val []byte, b BinWriter) {
	n, err := b.Write(val)
	if err != nil {
		panic(err)
	}
	if n != len(val) {
		panic(errors.New("short write"))
	}
}

func writeByte(data []byte, pos int, value byte) int {
	data[pos] = value
	return pos + 1
}

func writeUint16(data []byte, pos int, value uint16) int {
	data[pos] = byte(value)
	data[pos+1] = byte(value >> 8)
	return pos + 2
}

func writeUint32(data []byte, pos int, value uint32) int {
	data[pos] = byte(value)
	data[pos+1] = byte(value >> 8)
	data[pos+2] = byte(value >> 16)
	data[pos+3] = byte(value >> 24)
	return pos + 4
}

func writeUint64(data []byte, pos int, value uint64) int {
	data[pos] = byte(value)
	data[pos+1] = byte(value >> 8)
	data[pos+2] = byte(value >> 16)
	data[pos+3] = byte(value >> 24)
	data[pos+4] = byte(value >> 32)
	data[pos+5] = byte(value >> 40)
	data[pos+6] = byte(value >> 48)
	data[pos+7] = byte(value >> 56)
	return pos + 8
}

// lenEncIntSize returns the number of bytes required to encode a
// variable-length integer.
func lenEncIntSize(i uint64) int {
	switch {
	case i < 251:
		return 1
	case i < 1<<16:
		return 3
	case i < 1<<24:
		return 4
	default:
		return 9
	}
}

func writeLenEncInt(data []byte, pos int, i uint64) int {
	switch {
	case i < 251:
		data[pos] = byte(i)
		return pos + 1
	case i < 1<<16:
		data[pos] = 0xfc
		data[pos+1] = byte(i)
		data[pos+2] = byte(i >> 8)
		return pos + 3
	case i < 1<<24:
		data[pos] = 0xfd
		data[pos+1] = byte(i)
		data[pos+2] = byte(i >> 8)
		data[pos+3] = byte(i >> 16)
		return pos + 4
	default:
		data[pos] = 0xfe
		data[pos+1] = byte(i)
		data[pos+2] = byte(i >> 8)
		data[pos+3] = byte(i >> 16)
		data[pos+4] = byte(i >> 24)
		data[pos+5] = byte(i >> 32)
		data[pos+6] = byte(i >> 40)
		data[pos+7] = byte(i >> 48)
		data[pos+8] = byte(i >> 56)
		return pos + 9
	}
}

// ToMySQL converts Value to a mysql type value.
func (v Value) ToMySQL() ([]byte, error) {
	var out []byte
	pos := 0
	switch v.typ {
	case Null:
		// no-op
	case Int8:
		val, err := strconv.ParseInt(v.ToString(), 10, 8)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 1)
		writeByte(out, pos, uint8(val))
	case Uint8:
		val, err := strconv.ParseUint(v.ToString(), 10, 8)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 1)
		writeByte(out, pos, uint8(val))
	case Uint16:
		val, err := strconv.ParseUint(v.ToString(), 10, 16)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 2)
		writeUint16(out, pos, uint16(val))
	case Int16, Year:
		val, err := strconv.ParseInt(v.ToString(), 10, 16)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 2)
		writeUint16(out, pos, uint16(val))
	case Uint24, Uint32:
		val, err := strconv.ParseUint(v.ToString(), 10, 32)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 4)
		writeUint32(out, pos, uint32(val))
	case Int24, Int32:
		val, err := strconv.ParseInt(v.ToString(), 10, 32)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 4)
		writeUint32(out, pos, uint32(val))
	case Float32:
		val, err := strconv.ParseFloat(v.ToString(), 32)
		if err != nil {
			return []byte{}, err
		}
		bits := math.Float32bits(float32(val))
		out = make([]byte, 4)
		writeUint32(out, pos, bits)
	case Uint64:
		val, err := strconv.ParseUint(v.ToString(), 10, 64)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 8)
		writeUint64(out, pos, uint64(val))
	case Int64:
		val, err := strconv.ParseInt(v.ToString(), 10, 64)
		if err != nil {
			return []byte{}, err
		}
		out = make([]byte, 8)
		writeUint64(out, pos, uint64(val))
	case Float64:
		val, err := strconv.ParseFloat(v.ToString(), 64)
		if err != nil {
			return []byte{}, err
		}
		bits := math.Float64bits(val)
		out = make([]byte, 8)
		writeUint64(out, pos, bits)
	case Timestamp, Date, Datetime:
		if len(v.val) > 19 {
			out = make([]byte, 1+11)
			out[pos] = 0x0b
			pos++
			year, err := strconv.ParseUint(string(v.val[0:4]), 10, 16)
			if err != nil {
				return []byte{}, err
			}
			month, err := strconv.ParseUint(string(v.val[5:7]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			day, err := strconv.ParseUint(string(v.val[8:10]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			hour, err := strconv.ParseUint(string(v.val[11:13]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			minute, err := strconv.ParseUint(string(v.val[14:16]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			second, err := strconv.ParseUint(string(v.val[17:19]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			val := make([]byte, 6)
			count := copy(val, v.val[20:])
			for i := 0; i < (6 - count); i++ {
				val[count+i] = 0x30
			}
			microSecond, err := strconv.ParseUint(string(val), 10, 32)
			if err != nil {
				return []byte{}, err
			}
			pos = writeUint16(out, pos, uint16(year))
			pos = writeByte(out, pos, byte(month))
			pos = writeByte(out, pos, byte(day))
			pos = writeByte(out, pos, byte(hour))
			pos = writeByte(out, pos, byte(minute))
			pos = writeByte(out, pos, byte(second))
			pos = writeUint32(out, pos, uint32(microSecond))
		} else if len(v.val) > 10 {
			out = make([]byte, 1+7)
			out[pos] = 0x07
			pos++
			year, err := strconv.ParseUint(string(v.val[0:4]), 10, 16)
			if err != nil {
				return []byte{}, err
			}
			month, err := strconv.ParseUint(string(v.val[5:7]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			day, err := strconv.ParseUint(string(v.val[8:10]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			hour, err := strconv.ParseUint(string(v.val[11:13]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			minute, err := strconv.ParseUint(string(v.val[14:16]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			second, err := strconv.ParseUint(string(v.val[17:]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			pos = writeUint16(out, pos, uint16(year))
			pos = writeByte(out, pos, byte(month))
			pos = writeByte(out, pos, byte(day))
			pos = writeByte(out, pos, byte(hour))
			pos = writeByte(out, pos, byte(minute))
			pos = writeByte(out, pos, byte(second))
		} else if len(v.val) > 0 {
			out = make([]byte, 1+4)
			out[pos] = 0x04
			pos++
			year, err := strconv.ParseUint(string(v.val[0:4]), 10, 16)
			if err != nil {
				return []byte{}, err
			}
			month, err := strconv.ParseUint(string(v.val[5:7]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			day, err := strconv.ParseUint(string(v.val[8:]), 10, 8)
			if err != nil {
				return []byte{}, err
			}
			pos = writeUint16(out, pos, uint16(year))
			pos = writeByte(out, pos, byte(month))
			pos = writeByte(out, pos, byte(day))
		} else {
			out = make([]byte, 1)
			out[pos] = 0x00
		}
	case Time:
		if string(v.val) == "00:00:00" {
			out = make([]byte, 1)
			out[pos] = 0x00
		} else if strings.Contains(string(v.val), ".") {
			out = make([]byte, 1+12)
			out[pos] = 0x0c
			pos++

			sub1 := strings.Split(string(v.val), ":")
			if len(sub1) != 3 {
				err := fmt.Errorf("incorrect time value, ':' is not found")
				return []byte{}, err
			}
			sub2 := strings.Split(sub1[2], ".")
			if len(sub2) != 2 {
				err := fmt.Errorf("incorrect time value, '.' is not found")
				return []byte{}, err
			}

			var total []byte
			if strings.HasPrefix(sub1[0], "-") {
				out[pos] = 0x01
				total = []byte(sub1[0])
				total = total[1:]
			} else {
				out[pos] = 0x00
				total = []byte(sub1[0])
			}
			pos++

			h, err := strconv.ParseUint(string(total), 10, 32)
			if err != nil {
				return []byte{}, err
			}

			days := uint32(h) / 24
			hours := uint32(h) % 24
			minute := sub1[1]
			second := sub2[0]
			microSecond := sub2[1]

			minutes, err := strconv.ParseUint(minute, 10, 8)
			if err != nil {
				return []byte{}, err
			}

			seconds, err := strconv.ParseUint(second, 10, 8)
			if err != nil {
				return []byte{}, err
			}
			pos = writeUint32(out, pos, uint32(days))
			pos = writeByte(out, pos, byte(hours))
			pos = writeByte(out, pos, byte(minutes))
			pos = writeByte(out, pos, byte(seconds))

			val := make([]byte, 6)
			count := copy(val, microSecond)
			for i := 0; i < (6 - count); i++ {
				val[count+i] = 0x30
			}
			microSeconds, err := strconv.ParseUint(string(val), 10, 32)
			if err != nil {
				return []byte{}, err
			}
			pos = writeUint32(out, pos, uint32(microSeconds))
		} else if len(v.val) > 0 {
			out = make([]byte, 1+8)
			out[pos] = 0x08
			pos++

			sub1 := strings.Split(string(v.val), ":")
			if len(sub1) != 3 {
				err := fmt.Errorf("incorrect time value, ':' is not found")
				return []byte{}, err
			}

			var total []byte
			if strings.HasPrefix(sub1[0], "-") {
				out[pos] = 0x01
				total = []byte(sub1[0])
				total = total[1:]
			} else {
				out[pos] = 0x00
				total = []byte(sub1[0])
			}
			pos++

			h, err := strconv.ParseUint(string(total), 10, 32)
			if err != nil {
				return []byte{}, err
			}

			days := uint32(h) / 24
			hours := uint32(h) % 24
			minute := sub1[1]
			second := sub1[2]

			minutes, err := strconv.ParseUint(minute, 10, 8)
			if err != nil {
				return []byte{}, err
			}

			seconds, err := strconv.ParseUint(second, 10, 8)
			if err != nil {
				return []byte{}, err
			}
			pos = writeUint32(out, pos, uint32(days))
			pos = writeByte(out, pos, byte(hours))
			pos = writeByte(out, pos, byte(minutes))
			pos = writeByte(out, pos, byte(seconds))
		} else {
			err := fmt.Errorf("incorrect time value")
			return []byte{}, err
		}
	case Decimal, Text, VarChar, VarBinary, Char, Bit, Enum, Set, Geometry, Binary, TypeJSON:
		l := len(v.val)
		length := lenEncIntSize(uint64(l)) + l
		out = make([]byte, length)
		pos = writeLenEncInt(out, pos, uint64(l))
		copy(out[pos:], v.val)
	case Blob:
		l := len(v.val)
		length := lenEncIntSize(uint64(l)) + l + 1
		out = make([]byte, length)
		pos = writeLenEncInt(out, pos, uint64(l))
		copy(out[pos:], v.val)
	default:
		out = make([]byte, len(v.val))
		copy(out, v.val)
	}
	return out, nil
}

func ParseMySQLValues(buf *common.Buffer, typ querypb.Type) (interface{}, error) {
	switch typ {
	case Null:
		return nil, nil
	case Int8, Uint8:
		return buf.ReadU8()
	case Uint16:
		return buf.ReadU16()
	case Int16, Year:
		val, err := buf.ReadU16()
		if err != nil {
			return nil, err
		}
		return int16(val), nil
	case Uint24, Uint32:
		return buf.ReadU32()
	case Int24, Int32:
		val, err := buf.ReadU32()
		if err != nil {
			return nil, err
		}
		return int32(val), nil
	case Float32:
		val, err := buf.ReadU32()
		if err != nil {
			return nil, err
		}
		return math.Float32frombits(val), nil
	case Uint64:
		return buf.ReadU64()
	case Int64:
		val, err := buf.ReadU64()
		if err != nil {
			return nil, err
		}
		return int64(val), nil
	case Float64:
		val, err := buf.ReadU64()
		if err != nil {
			return nil, err
		}
		return math.Float64frombits(val), nil
	case Timestamp, Date, Datetime:
		var out []byte

		size, err := buf.ReadU8()
		if err != nil {
			return nil, err
		}
		switch size {
		case 0x00:
			out = append(out, ' ')
		case 0x0b:
			year, err := buf.ReadU16()
			if err != nil {
				return nil, err
			}

			month, err := buf.ReadU8()
			if err != nil {
				return nil, err
			}

			day, err := buf.ReadU8()
			if err != nil {
				return nil, err
			}

			hour, err := buf.ReadU8()
			if err != nil {
				return nil, err
			}

			minute, err := buf.ReadU8()
			if err != nil {
				return nil, err
			}

			second, err := buf.ReadU8()
			if err != nil {
				return nil, err
			}

			microSecond, err := buf.ReadU32()
			if err != nil {
				return nil, err
			}

			val := strconv.Itoa(int(year)) + "-" +
				strconv.Itoa(int(month)) + "-" +
				strconv.Itoa(int(day)) + " " +
				strconv.Itoa(int(hour)) + ":" +
				strconv.Itoa(int(minute)) + ":" +
				strconv.Itoa(int(second)) + "." +
				strconv.Itoa(int(microSecond))
			out = []byte(val)
			return out, nil
		case 0x07:
			year, err := buf.ReadU16()
			if err != nil {
				return nil, err
			}

			month, err := buf.ReadU8()
			if err != nil {
				return nil, err
			}

			day, err := buf.ReadU8()
			if err != nil {
				return nil, err
			}

			hour, err := buf.ReadU8()
			if err != nil {
				return nil, err
			}

			minute, err := buf.ReadU8()
			if err != nil {
				return nil, err
			}

			second, err := buf.ReadU8()
			if err != nil {
				return nil, err
			}

			val := strconv.Itoa(int(year)) + "-" +
				strconv.Itoa(int(month)) + "-" +
				strconv.Itoa(int(day)) + " " +
				strconv.Itoa(int(hour)) + ":" +
				strconv.Itoa(int(minute)) + ":" +
				strconv.Itoa(int(second))
			out = []byte(val)
			return out, nil
		case 0x04:
			year, err := buf.ReadU16()
			if err != nil {
				return nil, err
			}

			month, err := buf.ReadU8()
			if err != nil {
				return nil, err
			}

			day, err := buf.ReadU8()
			if err != nil {
				return nil, err
			}
			val := strconv.Itoa(int(year)) + "-" +
				strconv.Itoa(int(month)) + "-" +
				strconv.Itoa(int(day))
			out = []byte(val)
			return out, nil
		default:
			return nil, fmt.Errorf("datetime.error")
		}
	case Time:
		var out []byte

		size, err := buf.ReadU8()
		if err != nil {
			return nil, err
		}
		switch size {
		case 0x00:
			copy(out, "00:00:00")
		case 0x0c:
			isNegative, err := buf.ReadU8()
			if err != nil {
				return nil, err
			}

			days, err := buf.ReadU32()
			if err != nil {
				return nil, err
			}

			hour, err := buf.ReadU8()
			if err != nil {
				return nil, err
			}

			hours := uint32(hour) + days*uint32(24)

			minute, err := buf.ReadU8()
			if err != nil {
				return nil, err
			}

			second, err := buf.ReadU8()
			if err != nil {
				return nil, err
			}

			microSecond, err := buf.ReadU32()
			if err != nil {
				return nil, err
			}

			val := ""
			if isNegative == 0x01 {
				val += "-"
			}
			val += strconv.Itoa(int(hours)) + ":" +
				strconv.Itoa(int(minute)) + ":" +
				strconv.Itoa(int(second)) + "." +
				strconv.Itoa(int(microSecond))
			out = []byte(val)
			return out, nil
		case 0x08:
			isNegative, err := buf.ReadU8()
			if err != nil {
				return nil, err
			}

			days, err := buf.ReadU32()
			if err != nil {
				return nil, err
			}

			hour, err := buf.ReadU8()
			if err != nil {
				return nil, err
			}

			hours := uint32(hour) + days*uint32(24)

			minute, err := buf.ReadU8()
			if err != nil {
				return nil, err
			}

			second, err := buf.ReadU8()
			if err != nil {
				return nil, err
			}

			val := ""
			if isNegative == 0x01 {
				val += "-"
			}
			val += strconv.Itoa(int(hours)) + ":" +
				strconv.Itoa(int(minute)) + ":" +
				strconv.Itoa(int(second))
			out = []byte(val)
			return out, nil
		default:
			return nil, fmt.Errorf("time.error")
		}
	case Decimal, Text, Blob, VarChar, Char,
		Bit, Enum, Set, Geometry, TypeJSON:
		return buf.ReadLenEncodeString()
	case VarBinary, Binary:
		return buf.ReadLenEncodeBytes()
	default:
		return nil, fmt.Errorf("type.unhandle.error")
	}
	return nil, fmt.Errorf("type.unhandle.error")
}

func encodeBytesSQL(val []byte, b BinWriter) {
	writebyte('\'', b)
	for _, ch := range val {
		if encodedChar := SQLEncodeMap[ch]; encodedChar == DontEscape {
			writebyte(ch, b)
		} else {
			writebyte('\\', b)
			writebyte(encodedChar, b)
		}
	}
	writebyte('\'', b)
}

func encodeBytesASCII(val []byte, b BinWriter) {
	writebyte('\'', b)
	encoder := base64.NewEncoder(base64.StdEncoding, b)
	encoder.Write(val)
	encoder.Close()
	writebyte('\'', b)
}

// SQLEncodeMap specifies how to escape binary data with '\'.
// Complies to http://dev.mysql.com/doc/refman/5.1/en/string-syntax.html
var SQLEncodeMap [256]byte

// SQLDecodeMap is the reverse of SQLEncodeMap
var SQLDecodeMap [256]byte

var encodeRef = map[byte]byte{
	'\x00': '0',
	'\'':   '\'',
	'"':    '"',
	'\b':   'b',
	'\n':   'n',
	'\r':   'r',
	'\t':   't',
	26:     'Z', // ctl-Z
	'\\':   '\\',
}

func init() {
	for i := range SQLEncodeMap {
		SQLEncodeMap[i] = DontEscape
		SQLDecodeMap[i] = DontEscape
	}
	for i := range SQLEncodeMap {
		if to, ok := encodeRef[byte(i)]; ok {
			SQLEncodeMap[byte(i)] = to
			SQLDecodeMap[to] = byte(i)
		}
	}
}
