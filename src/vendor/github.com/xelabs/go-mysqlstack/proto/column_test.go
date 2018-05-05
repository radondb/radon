/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package proto

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/common"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

func TestColumnCount(t *testing.T) {
	payload := []byte{
		0x02,
	}

	want := uint64(2)
	got, err := ColumnCount(payload)
	assert.Nil(t, err)
	assert.Equal(t, want, got)
}

func TestColumn(t *testing.T) {
	want := &querypb.Field{
		Database:     "test",
		Table:        "t1",
		OrgTable:     "t1",
		Name:         "a",
		OrgName:      "a",
		Charset:      11,
		ColumnLength: 11,
		Type:         sqltypes.Int32,
		Flags:        11,
	}

	datas := PackColumn(want)
	got, err := UnpackColumn(datas)
	assert.Nil(t, err)
	assert.Equal(t, want, got)
}

func TestColumnUnPackError(t *testing.T) {
	// NULL
	f0 := func(buff *common.Buffer) {
	}

	// Write catalog.
	f1 := func(buff *common.Buffer) {
		buff.WriteLenEncodeString("def")
	}

	// Write schema.
	f2 := func(buff *common.Buffer) {
		buff.WriteLenEncodeString("sbtest")
	}

	// Write table.
	f3 := func(buff *common.Buffer) {
		buff.WriteLenEncodeString("table1")
	}

	// Write org table.
	f4 := func(buff *common.Buffer) {
		buff.WriteLenEncodeString("orgtable1")
	}

	// Write Name.
	f5 := func(buff *common.Buffer) {
		buff.WriteLenEncodeString("name")
	}

	// Write Org Name.
	f6 := func(buff *common.Buffer) {
		buff.WriteLenEncodeString("name")
	}

	// Write length.
	f7 := func(buff *common.Buffer) {
		buff.WriteLenEncode(0x0c)
	}

	// Write Charset.
	f8 := func(buff *common.Buffer) {
		buff.WriteU16(uint16(1))
	}

	// Write Column length.
	f9 := func(buff *common.Buffer) {
		buff.WriteU32(uint32(1))
	}

	// Write type.
	f10 := func(buff *common.Buffer) {
		buff.WriteU8(0x01)
	}

	// Write flags
	f11 := func(buff *common.Buffer) {
		buff.WriteU16(uint16(1))
	}

	buff := common.NewBuffer(32)
	fs := []func(buff *common.Buffer){f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11}
	for i := 0; i < len(fs); i++ {
		_, err := UnpackColumn(buff.Datas())
		assert.NotNil(t, err)
		fs[i](buff)
	}

	{
		_, err := UnpackColumn(buff.Datas())
		assert.NotNil(t, err)
	}
}
