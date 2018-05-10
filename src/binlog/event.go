/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package binlog

import (
	"fmt"
	"hash/crc32"

	"github.com/xelabs/go-mysqlstack/common"
)

// Event binlog event.
type Event struct {
	// An identifier that describes the event type.
	Type    string
	Schema  string
	Query   string
	Version uint16
	// The GTID of this event.
	Timestamp uint64
	// The name of the file that is being listed.
	LogName string
	// The position at which the event occurs.
	Pos int64
	// The position at which the next event begins, which is equal to Pos plus the size of the event.
	EndLogPos int64
}

const (
	v1 = 1
)

func packEventv1(e *Event) []byte {
	crc32 := crc32.ChecksumIEEE(common.StringToBytes(e.Query))

	buf := common.NewBuffer(256)
	buf.WriteU16(v1)
	buf.WriteU64(e.Timestamp)
	buf.WriteLenEncodeString(e.Type)
	buf.WriteLenEncodeString(e.Schema)
	buf.WriteLenEncodeString(e.Query)
	buf.WriteU32(crc32)
	return buf.Datas()
}

func unpackEvent(datas []byte) (*Event, error) {
	var err error
	e := &Event{}

	buf := common.ReadBuffer(datas)
	e.Version, err = buf.ReadU16()
	if err != nil {
		return nil, fmt.Errorf("event.read.version.error:%v", err)
	}
	switch e.Version {
	case v1:
		// GTID.
		if e.Timestamp, err = buf.ReadU64(); err != nil {
			return nil, err
		}

		// Typee.
		if e.Type, err = buf.ReadLenEncodeString(); err != nil {
			return nil, err
		}

		// Schema.
		if e.Schema, err = buf.ReadLenEncodeString(); err != nil {
			return nil, err
		}

		// Query.
		if e.Query, err = buf.ReadLenEncodeString(); err != nil {
			return nil, err
		}

		// CRC32.
		var crc1, crc2 uint32
		crc1 = crc32.ChecksumIEEE(common.StringToBytes(e.Query))
		if crc2, err = buf.ReadU32(); err != nil {
			return nil, err
		}
		if crc1 != crc2 {
			return nil, fmt.Errorf("event.crc32.check[%v].read[%v].query[%v]", crc1, crc2, e.Query)
		}
		return e, nil
	default:
		return nil, fmt.Errorf("event.unknow.version[%v]", e.Version)
	}
}
