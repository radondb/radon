package mysql

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mysqlTestSuite struct {
}

var _ = &mysqlTestSuite{}

// no need now
/*
func (t *mysqlTestSuite) SetUpSuite(c *check.C) {

}

func (t *mysqlTestSuite) TearDownSuite(c *check.C) {

}
*/

func TestMysqlGTIDInterval(t *testing.T) {
	i, err := parseInterval("1-2")
	assert.Nil(t, err)
	assert.EqualValues(t, Interval{1, 3}, i)

	i, err = parseInterval("1")
	assert.Nil(t, err)
	assert.EqualValues(t, Interval{1, 2}, i)

	i, err = parseInterval("1-1")
	assert.Nil(t, err)
	assert.EqualValues(t, Interval{1, 2}, i)

	i, err = parseInterval("1-2")
	assert.Nil(t, err)
}

func TestMysqlGTIDIntervalSlice(t *testing.T) {
	i := IntervalSlice{Interval{1, 2}, Interval{2, 4}, Interval{2, 3}}
	i.Sort()
	assert.EqualValues(t, IntervalSlice{Interval{1, 2}, Interval{2, 3}, Interval{2, 4}}, i)
	n := i.Normalize()
	assert.EqualValues(t, IntervalSlice{Interval{1, 4}}, n)

	i = IntervalSlice{Interval{1, 2}, Interval{3, 5}, Interval{1, 3}}
	i.Sort()
	assert.EqualValues(t, IntervalSlice{Interval{1, 2}, Interval{1, 3}, Interval{3, 5}}, i)
	n = i.Normalize()
	assert.EqualValues(t, IntervalSlice{Interval{1, 5}}, n)

	i = IntervalSlice{Interval{1, 2}, Interval{4, 5}, Interval{1, 3}}
	i.Sort()
	assert.EqualValues(t, IntervalSlice{Interval{1, 2}, Interval{1, 3}, Interval{4, 5}}, i)
	n = i.Normalize()
	assert.EqualValues(t, IntervalSlice{Interval{1, 3}, Interval{4, 5}}, n)

	i = IntervalSlice{Interval{1, 4}, Interval{2, 3}}
	i.Sort()
	assert.EqualValues(t, IntervalSlice{Interval{1, 4}, Interval{2, 3}}, i)
	n = i.Normalize()
	assert.EqualValues(t, IntervalSlice{Interval{1, 4}}, n)

	n1 := IntervalSlice{Interval{1, 3}, Interval{4, 5}}
	n2 := IntervalSlice{Interval{1, 2}}

	assert.True(t, n1.Contain(n2))
	assert.False(t, n2.Contain(n1))

	n1 = IntervalSlice{Interval{1, 3}, Interval{4, 5}}
	n2 = IntervalSlice{Interval{1, 6}}

	assert.False(t, n1.Contain(n2))
	assert.True(t, n2.Contain(n1))
}

func TestMysqlGTIDCodec(t *testing.T) {
	us, err := ParseUUIDSet("de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2")
	assert.Nil(t, err)

	assert.Equal(t, "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2", us.String())

	buf := us.Encode()
	err = us.Decode(buf)
	assert.Nil(t, err)

	gs, err := ParseMysqlGTIDSet("de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2,de278ad0-2106-11e4-9f8e-6edd0ca20948:1-2")
	assert.Nil(t, err)

	buf = gs.Encode()
	o, err := DecodeMysqlGTIDSet(buf)
	assert.Nil(t, err)
	assert.Equal(t, o, gs)
}

func TestMysqlUpdate(t *testing.T) {
	g1, err := ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57")
	assert.Nil(t, err)

	g1.Update("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-58")

	assert.Equal(t, "3E11FA47-71CA-11E1-9E33-C80AA9429562:21-58", strings.ToUpper(g1.String()))
}

func TestMysqlGTIDContain(t *testing.T) {
	g1, err := ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:23")
	assert.Nil(t, err)

	g2, err := ParseMysqlGTIDSet("3E11FA47-71CA-11E1-9E33-C80AA9429562:21-57")
	assert.Nil(t, err)

	assert.True(t, g2.Contain(g1))
	assert.False(t, g1.Contain(g2))
}

func TestMysqlParseBinaryInt8(t *testing.T) {
	i8 := ParseBinaryInt8([]byte{128})
	assert.Equal(t, int8(-128), i8)
}

func TestMysqlParseBinaryUint8(t *testing.T) {
	u8 := ParseBinaryUint8([]byte{128})
	assert.Equal(t, uint8(128), u8)
}

func TestMysqlParseBinaryInt16(t *testing.T) {
	i16 := ParseBinaryInt16([]byte{1, 128})
	assert.Equal(t, int16(-128*256+1), i16)
}

func TestMysqlParseBinaryUint16(t *testing.T) {
	u16 := ParseBinaryUint16([]byte{1, 128})
	assert.Equal(t, uint16(128*256+1), u16)
}

func TestMysqlParseBinaryInt24(t *testing.T) {
	i32 := ParseBinaryInt24([]byte{1, 2, 128})
	assert.Equal(t, int32(-128*65536+2*256+1), i32)
}

func TestMysqlParseBinaryUint24(t *testing.T) {
	u32 := ParseBinaryUint24([]byte{1, 2, 128})
	assert.Equal(t, uint32(128*65536+2*256+1), u32)
}

func TestMysqlParseBinaryInt32(t *testing.T) {
	i32 := ParseBinaryInt32([]byte{1, 2, 3, 128})
	assert.Equal(t, int32(-128*16777216+3*65536+2*256+1), i32)
}

func TestMysqlParseBinaryUint32(t *testing.T) {
	u32 := ParseBinaryUint32([]byte{1, 2, 3, 128})
	assert.Equal(t, uint32(128*16777216+3*65536+2*256+1), u32)
}

func TestMysqlParseBinaryInt64(t *testing.T) {
	i64 := ParseBinaryInt64([]byte{1, 2, 3, 4, 5, 6, 7, 128})
	assert.Equal(t, -128*int64(72057594037927936)+7*int64(281474976710656)+6*int64(1099511627776)+5*int64(4294967296)+4*16777216+3*65536+2*256+1, i64)
}

func TestMysqlParseBinaryUint64(t *testing.T) {
	u64 := ParseBinaryUint64([]byte{1, 2, 3, 4, 5, 6, 7, 128})
	assert.Equal(t, 128*uint64(72057594037927936)+7*uint64(281474976710656)+6*uint64(1099511627776)+5*uint64(4294967296)+4*16777216+3*65536+2*256+1, u64)
}

func TestErrorCode(t *testing.T) {
	tbls := []struct {
		msg  string
		code int
	}{
		{"ERROR 1094 (HY000): Unknown thread id: 1094", 1094},
		{"error string", 0},
		{"abcdefg", 0},
		{"123455 ks094", 0},
		{"ERROR 1046 (3D000): Unknown error 1046", 1046},
	}
	for _, v := range tbls {
		assert.Equal(t, v.code, ErrorCode(v.msg))
	}
}

func TestMysqlNullDecode(t *testing.T) {
	_, isNull, n := LengthEncodedInt([]byte{0xfb})

	assert.True(t, isNull)
	assert.Equal(t, 1, n)
}
