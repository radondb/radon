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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

func TestStatementPrepare(t *testing.T) {
	want := &Statement{
		ID:          5,
		ColumnCount: 2,
		ParamCount:  3,
		Warnings:    1,
	}
	datas := PackStatementPrepare(want)
	got, err := UnPackStatementPrepare(datas)
	assert.Nil(t, err)
	assert.Equal(t, want, got)
}

func TestStatementPrepareUnPackError(t *testing.T) {
	// NULL
	f0 := func(buff *common.Buffer) {
	}

	// Write ok.
	f1 := func(buff *common.Buffer) {
		buff.WriteU8(OK_PACKET)
	}

	// Write ID.
	f2 := func(buff *common.Buffer) {
		buff.WriteU32(1)
	}

	// Write Column count.
	f3 := func(buff *common.Buffer) {
		buff.WriteU16(1)
	}

	// Write param count.
	f4 := func(buff *common.Buffer) {
		buff.WriteU16(2)
	}

	// Write reserved.
	f5 := func(buff *common.Buffer) {
		buff.WriteU8(2)
	}

	f6 := func(buff *common.Buffer) {
		buff.WriteU8(2)
	}

	buff := common.NewBuffer(32)
	fs := []func(buff *common.Buffer){f0, f1, f2, f3, f4, f5, f6}
	for i := 0; i < len(fs); i++ {
		_, err := UnPackStatementPrepare(buff.Datas())
		assert.NotNil(t, err)
		fs[i](buff)
	}
}

func TestStatementExecute(t *testing.T) {
	id := uint32(11)
	values := []sqltypes.Value{
		sqltypes.MakeTrusted(sqltypes.Int32, []byte("10")),
		sqltypes.MakeTrusted(sqltypes.VarChar, []byte("xx10xx")),
		sqltypes.MakeTrusted(sqltypes.Null, nil),
		sqltypes.MakeTrusted(sqltypes.Text, []byte{}),
		sqltypes.MakeTrusted(sqltypes.Datetime, []byte(time.Now().Format("2006-01-02 15:04:05"))),
	}

	datas, err := PackStatementExecute(id, values)
	assert.Nil(t, err)

	parseFn := func(*common.Buffer, querypb.Type) (interface{}, error) {
		return nil, nil
	}
	got, err := UnPackStatementExecute(datas, 4, parseFn)
	assert.Nil(t, err)
	assert.NotNil(t, got)
}

func TestStatementExecuteUnPackError(t *testing.T) {
	// NULL
	f0 := func(buff *common.Buffer) {
	}

	// Write ID.
	f1 := func(buff *common.Buffer) {
		buff.WriteU32(1)
	}

	// Cursor type.
	f2 := func(buff *common.Buffer) {
		buff.WriteU8(1)
	}

	// Iteration count.
	f3 := func(buff *common.Buffer) {
		buff.WriteU32(1)
	}

	// Write param count.
	f4 := func(buff *common.Buffer) {
		buff.WriteU16(2)
	}

	// Write null bits.
	f5 := func(buff *common.Buffer) {
		buff.WriteBytes([]byte{0x00})
	}

	// newParameterBoundFlag.
	f6 := func(buff *common.Buffer) {
		buff.WriteU8(0x01)
	}

	parseFn := func(*common.Buffer, querypb.Type) (interface{}, error) {
		return nil, errors.New("mock.error")
	}

	buff := common.NewBuffer(32)
	fs := []func(buff *common.Buffer){f0, f1, f2, f3, f4, f5, f6}
	for i := 0; i < len(fs); i++ {
		_, err := UnPackStatementExecute(buff.Datas(), 1, parseFn)
		assert.NotNil(t, err)
		fs[i](buff)
	}
}
