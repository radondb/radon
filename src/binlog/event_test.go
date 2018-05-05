/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package binlog

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/common"
)

func TestEventv1(t *testing.T) {
	e := &Event{
		Type:  "INSERT",
		Query: "insert into t1 values(1)",
	}

	datas := packEventv1(e)
	e1, err := unpackEvent(datas)
	assert.Nil(t, err)
	assert.Equal(t, e.Query, e1.Query)
	log.Printf("...%+v", e1)
}

func TestEventv1Error(t *testing.T) {
	// version error.
	{
		buf := common.NewBuffer(128)
		buf.WriteU8(v1)
		_, err := unpackEvent(buf.Datas())
		assert.NotNil(t, err)
	}

	// timestamp error.
	{
		buf := common.NewBuffer(128)
		buf.WriteU16(v1)
		buf.WriteLenEncodeString("xx")
		_, err := unpackEvent(buf.Datas())
		assert.NotNil(t, err)
	}

	// typ error.
	{
		buf := common.NewBuffer(128)
		buf.WriteU16(v1)
		buf.WriteU64(0)
		buf.WriteU64(65535)
		_, err := unpackEvent(buf.Datas())
		assert.NotNil(t, err)
	}

	// query error.
	{
		buf := common.NewBuffer(128)
		buf.WriteU16(v1)
		buf.WriteU64(0)
		buf.WriteLenEncodeString("INSERT")
		buf.WriteU64(65535)
		_, err := unpackEvent(buf.Datas())
		assert.NotNil(t, err)
	}

	// crc error.
	{
		buf := common.NewBuffer(128)
		buf.WriteU16(v1)
		buf.WriteU64(0)
		buf.WriteLenEncodeString("INSERT")
		buf.WriteLenEncodeString("INSERT")
		buf.WriteU8(124)
		_, err := unpackEvent(buf.Datas())
		assert.NotNil(t, err)
	}

	// crc check error.
	{
		buf := common.NewBuffer(128)
		buf.WriteU16(v1)
		buf.WriteU64(0)
		buf.WriteLenEncodeString("INSERT")
		buf.WriteLenEncodeString("INSERT")
		buf.WriteU32(124)
		_, err := unpackEvent(buf.Datas())
		assert.NotNil(t, err)
	}

	// unknow version.
	{
		buf := common.NewBuffer(128)
		buf.WriteU16(125)
		_, err := unpackEvent(buf.Datas())
		assert.NotNil(t, err)
	}
}
