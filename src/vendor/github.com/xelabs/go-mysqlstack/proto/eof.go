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
	// EOF_PACKET is the EOF packet.
	EOF_PACKET byte = 0xfe
)

// EOF used for EOF packet.
type EOF struct {
	Header      byte // 0x00
	Warnings    uint16
	StatusFlags uint16
}

// UnPackEOF used to unpack the EOF packet.
// https://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html
// This method unsed.
func UnPackEOF(data []byte) (*EOF, error) {
	var err error
	e := &EOF{}
	buf := common.ReadBuffer(data)

	// header
	if e.Header, err = buf.ReadU8(); err != nil {
		return nil, sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "invalid eof packet header: %v", data)
	}
	if e.Header != EOF_PACKET {
		return nil, sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "invalid oeof packet header: %v", e.Header)
	}

	// Warnings
	if e.Warnings, err = buf.ReadU16(); err != nil {
		return nil, sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "invalid eof packet warnings: %v", data)
	}

	// Status
	if e.StatusFlags, err = buf.ReadU16(); err != nil {
		return nil, sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "invalid eof packet statusflags: %v", data)
	}
	return e, nil
}

// PackEOF used to pack the EOF packet.
func PackEOF(e *EOF) []byte {
	buf := common.NewBuffer(64)

	// EOF
	buf.WriteU8(EOF_PACKET)

	// warnings
	buf.WriteU16(e.Warnings)

	// status
	buf.WriteU16(e.StatusFlags)
	return buf.Datas()
}
