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

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
)

func TestEOF(t *testing.T) {
	want := &EOF{}
	want.Header = EOF_PACKET
	want.StatusFlags = 1
	want.Warnings = 2
	data := PackEOF(want)

	got, err := UnPackEOF(data)
	assert.Nil(t, err)
	assert.Equal(t, want, got)
}

func TestEOFUnPackError(t *testing.T) {
	// header error
	{
		buff := common.NewBuffer(32)
		// header
		buff.WriteU8(0x99)
		_, err := UnPackEOF(buff.Datas())
		assert.NotNil(t, err)
	}

	// NULL
	f0 := func(buff *common.Buffer) {
	}

	// Write EOF header.
	f1 := func(buff *common.Buffer) {
		buff.WriteU8(0xfe)
	}

	// Write Status.
	f2 := func(buff *common.Buffer) {
		buff.WriteU16(0x01)
	}

	buff := common.NewBuffer(32)
	fs := []func(buff *common.Buffer){f0, f1, f2}
	for i := 0; i < len(fs); i++ {
		_, err := UnPackEOF(buff.Datas())
		assert.NotNil(t, err)
		fs[i](buff)
	}

	{
		_, err := UnPackEOF(buff.Datas())
		assert.NotNil(t, err)
	}
}
