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
	"github.com/xelabs/go-mysqlstack/sqldb"
)

func TestERR(t *testing.T) {
	{
		buff := common.NewBuffer(32)

		// header
		buff.WriteU8(0xff)
		// error_code
		buff.WriteU16(0x01)
		// sql_state_marker
		buff.WriteString("#")
		// sql_state
		buff.WriteString("ABCDE")
		buff.WriteString("ERROR")

		e := &ERR{}
		e.Header = 0xff
		e.ErrorCode = 0x1
		e.SQLState = "ABCDE"
		e.ErrorMessage = "ERROR"
		want := sqldb.NewSQLError1(e.ErrorCode, e.SQLState, "%s", e.ErrorMessage)
		got := UnPackERR(buff.Datas())
		assert.Equal(t, want, got)
	}

	{
		e := &ERR{}
		e.Header = 0xff
		e.ErrorCode = 0x1
		e.ErrorMessage = "ERROR"
		datas := PackERR(e)
		want := sqldb.NewSQLError1(e.ErrorCode, e.SQLState, "%s", e.ErrorMessage)
		got := UnPackERR(datas)
		assert.Equal(t, want, got)
	}
}

func TestERRUnPackError(t *testing.T) {
	// header error
	{
		buff := common.NewBuffer(32)

		// header
		buff.WriteU8(0x01)

		err := UnPackERR(buff.Datas())
		assert.NotNil(t, err)
	}

	// NULL
	f0 := func(buff *common.Buffer) {
	}

	// Write error header.
	f1 := func(buff *common.Buffer) {
		buff.WriteU8(0xff)
	}

	// Write error code.
	f2 := func(buff *common.Buffer) {
		buff.WriteU16(0x01)
	}

	// Write SQLStateMarker.
	f3 := func(buff *common.Buffer) {
		buff.WriteU8('#')
	}

	// Write SQLState.
	f4 := func(buff *common.Buffer) {
		buff.WriteString("xxxxx")
	}

	buff := common.NewBuffer(32)
	fs := []func(buff *common.Buffer){f0, f1, f2, f3, f4}
	for i := 0; i < len(fs); i++ {
		err := UnPackERR(buff.Datas())
		assert.NotNil(t, err)
		fs[i](buff)
	}
}
