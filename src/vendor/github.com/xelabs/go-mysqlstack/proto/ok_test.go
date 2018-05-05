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
)

func TestOK(t *testing.T) {
	{
		buff := common.NewBuffer(32)

		// header
		buff.WriteU8(0x00)
		// affected_rows
		buff.WriteLenEncode(uint64(3))
		// last_insert_id
		buff.WriteLenEncode(uint64(40000000000))

		// status_flags
		buff.WriteU16(0x01)
		// warnings
		buff.WriteU16(0x02)

		want := &OK{}
		want.AffectedRows = 3
		want.LastInsertID = 40000000000
		want.StatusFlags = 1
		want.Warnings = 2

		got, err := UnPackOK(buff.Datas())
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	}

	{
		want := &OK{}
		want.AffectedRows = 3
		want.LastInsertID = 40000000000
		want.StatusFlags = 1
		want.Warnings = 2
		datas := PackOK(want)

		got, err := UnPackOK(datas)
		assert.Nil(t, err)
		assert.Equal(t, want, got)
	}
}

func TestOKUnPackError(t *testing.T) {
	// header error
	{
		buff := common.NewBuffer(32)
		// header
		buff.WriteU8(0x99)
		_, err := UnPackOK(buff.Datas())
		assert.NotNil(t, err)
	}

	// NULL
	f0 := func(buff *common.Buffer) {
	}

	// Write OK header.
	f1 := func(buff *common.Buffer) {
		buff.WriteU8(0x00)
	}

	// Write AffectedRows.
	f2 := func(buff *common.Buffer) {
		buff.WriteLenEncode(uint64(3))
	}

	// Write LastInsertID.
	f3 := func(buff *common.Buffer) {
		buff.WriteLenEncode(uint64(3))
	}

	// Write Status.
	f4 := func(buff *common.Buffer) {
		buff.WriteU16(0x01)
	}

	buff := common.NewBuffer(32)
	fs := []func(buff *common.Buffer){f0, f1, f2, f3, f4}
	for i := 0; i < len(fs); i++ {
		_, err := UnPackOK(buff.Datas())
		assert.NotNil(t, err)
		fs[i](buff)
	}

	{
		_, err := UnPackOK(buff.Datas())
		assert.NotNil(t, err)
	}
}
