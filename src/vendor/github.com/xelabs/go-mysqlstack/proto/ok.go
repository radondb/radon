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
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
)

const (
	// OK_PACKET is the OK byte.
	OK_PACKET byte = 0x00
)

// OK used for OK packet.
type OK struct {
	Header       byte // 0x00
	AffectedRows uint64
	LastInsertID uint64
	StatusFlags  uint16
	Warnings     uint16
}

// UnPackOK used to unpack the OK packet.
// https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
func UnPackOK(data []byte) (*OK, error) {
	var err error
	o := &OK{}
	buf := common.ReadBuffer(data)

	// header
	if o.Header, err = buf.ReadU8(); err != nil {
		return nil, sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "invalid ok packet header: %v", data)
	}
	if o.Header != OK_PACKET {
		return nil, sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "invalid ok packet header: %v", o.Header)
	}

	// AffectedRows
	if o.AffectedRows, err = buf.ReadLenEncode(); err != nil {
		return nil, sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "invalid ok packet affectedrows: %v", data)
	}

	// LastInsertID
	if o.LastInsertID, err = buf.ReadLenEncode(); err != nil {
		return nil, sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "invalid ok packet lastinsertid: %v", data)
	}

	// Status
	if o.StatusFlags, err = buf.ReadU16(); err != nil {
		return nil, sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "invalid ok packet statusflags: %v", data)
	}

	// Warnings
	if o.Warnings, err = buf.ReadU16(); err != nil {
		return nil, sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "invalid ok packet warnings: %v", data)
	}
	return o, nil
}

// PackOK used to pack the OK packet.
func PackOK(o *OK) []byte {
	buf := common.NewBuffer(64)

	// OK
	buf.WriteU8(OK_PACKET)

	// affected rows
	buf.WriteLenEncode(o.AffectedRows)

	// last insert id
	buf.WriteLenEncode(o.LastInsertID)

	// status
	buf.WriteU16(o.StatusFlags)

	// warnings
	buf.WriteU16(o.Warnings)
	return buf.Datas()
}
