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
	// ERR_PACKET is the error packet byte.
	ERR_PACKET byte = 0xff
)

// ERR is the error packet.
type ERR struct {
	Header       byte // always 0xff
	ErrorCode    uint16
	SQLState     string
	ErrorMessage string
}

// UnPackERR parses the error packet and returns a sqldb.SQLError.
// https://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html
func UnPackERR(data []byte) error {
	var err error
	e := &ERR{}
	buf := common.ReadBuffer(data)
	if e.Header, err = buf.ReadU8(); err != nil {
		return sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "invalid error packet header: %v", data)
	}
	if e.Header != ERR_PACKET {
		return sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "invalid error packet header: %v", e.Header)
	}
	if e.ErrorCode, err = buf.ReadU16(); err != nil {
		return sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "invalid error packet code: %v", data)
	}

	// Skip SQLStateMarker
	if _, err = buf.ReadString(1); err != nil {
		return sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "invalid error packet marker: %v", data)
	}
	if e.SQLState, err = buf.ReadString(5); err != nil {
		return sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "invalid error packet sqlstate: %v", data)
	}
	msgLen := len(data) - buf.Seek()
	if e.ErrorMessage, err = buf.ReadString(msgLen); err != nil {
		return sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "invalid error packet message: %v", data)
	}
	return sqldb.NewSQLError1(e.ErrorCode, e.SQLState, "%s", e.ErrorMessage)
}

// PackERR used to pack the error packet.
func PackERR(e *ERR) []byte {
	buf := common.NewBuffer(64)

	buf.WriteU8(ERR_PACKET)

	// error code
	buf.WriteU16(e.ErrorCode)

	// sql-state marker #
	buf.WriteU8('#')

	// sql-state (?) 5 ascii bytes
	if e.SQLState == "" {
		e.SQLState = "HY000"
	}
	if len(e.SQLState) != 5 {
		panic("sqlState has to be 5 characters long")
	}
	buf.WriteString(e.SQLState)

	// error msg
	buf.WriteString(e.ErrorMessage)
	return buf.Datas()
}
