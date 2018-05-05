/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package driver

import (
	"errors"

	"github.com/xelabs/go-mysqlstack/common"
	"github.com/xelabs/go-mysqlstack/proto"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

var _ Rows = &TextRows{}

// Rows presents row cursor interface.
type Rows interface {
	Next() bool
	Close() error
	Datas() []byte
	Bytes() int
	RowsAffected() uint64
	LastInsertID() uint64
	LastError() error
	Fields() []*querypb.Field
	RowValues() ([]sqltypes.Value, error)
}

// TextRows presents row tuple.
type TextRows struct {
	c            Conn
	end          bool
	err          error
	data         []byte
	bytes        int
	rowsAffected uint64
	insertID     uint64
	buffer       *common.Buffer
	fields       []*querypb.Field
}

// NewTextRows creates TextRows.
func NewTextRows(c Conn) *TextRows {
	return &TextRows{
		c:      c,
		buffer: common.NewBuffer(8),
	}
}

// Next implements the Rows interface.
// http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow
func (r *TextRows) Next() bool {
	defer func() {
		if r.err != nil {
			r.c.Cleanup()
		}
	}()

	if r.end {
		return false
	}

	// if fields count is 0
	// the packet is OK-Packet without Resultset.
	if len(r.fields) == 0 {
		r.end = true
		return false
	}

	if r.data, r.err = r.c.NextPacket(); r.err != nil {
		r.end = true
		return false
	}

	switch r.data[0] {
	case proto.EOF_PACKET:
		// This packet may be one of two kinds:
		// - an EOF packet,
		// - an OK packet with an EOF header if
		// sqldb.CLIENT_DEPRECATE_EOF is set.
		r.end = true
		return false

	case proto.ERR_PACKET:
		r.err = proto.UnPackERR(r.data)
		r.end = true
		return false
	}
	r.buffer.Reset(r.data)
	return true
}

// Close drain the rest packets and check the error.
func (r *TextRows) Close() error {
	for r.Next() {
	}
	return r.LastError()
}

// RowValues implements the Rows interface.
// https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow
func (r *TextRows) RowValues() ([]sqltypes.Value, error) {
	if r.fields == nil {
		return nil, errors.New("rows.fields is NIL")
	}

	empty := true
	colNumber := len(r.fields)
	result := make([]sqltypes.Value, colNumber)
	for i := 0; i < colNumber; i++ {
		v, err := r.buffer.ReadLenEncodeBytes()
		if err != nil {
			r.c.Cleanup()
			return nil, err
		}

		if v != nil {
			r.bytes += len(v)
			result[i] = sqltypes.MakeTrusted(r.fields[i].Type, v)
			empty = false
		}
	}
	if empty {
		return nil, nil
	}
	return result, nil
}

// Datas implements the Rows interface.
func (r *TextRows) Datas() []byte {
	return r.buffer.Datas()
}

// Fields implements the Rows interface.
func (r *TextRows) Fields() []*querypb.Field {
	return r.fields
}

// Bytes returns all the memory usage which read by this row cursor.
func (r *TextRows) Bytes() int {
	return r.bytes
}

// RowsAffected implements the Rows interface.
func (r *TextRows) RowsAffected() uint64 {
	return r.rowsAffected
}

// LastInsertID implements the Rows interface.
func (r *TextRows) LastInsertID() uint64 {
	return r.insertID
}

// LastError implements the Rows interface.
func (r *TextRows) LastError() error {
	return r.err
}
