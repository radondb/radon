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
	"fmt"

	"github.com/xelabs/go-mysqlstack/proto"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

var _ Rows = &TextRows{}

type RowMode int

const (
	TextRowMode RowMode = iota
	BinaryRowMode
)

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

// BaseRows --
type BaseRows struct {
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

// TextRows presents row tuple.
type TextRows struct {
	BaseRows
}

// BinaryRows presents binary row tuple.
type BinaryRows struct {
	BaseRows
}

// Next implements the Rows interface.
// http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow
func (r *BaseRows) Next() bool {
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
func (r *BaseRows) Close() error {
	for r.Next() {
	}
	return r.LastError()
}

// RowValues implements the Rows interface.
// https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::ResultsetRow
func (r *BaseRows) RowValues() ([]sqltypes.Value, error) {
	if r.fields == nil {
		return nil, errors.New("rows.fields is NIL")
	}

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
		}
	}
	return result, nil
}

// Datas implements the Rows interface.
func (r *BaseRows) Datas() []byte {
	return r.buffer.Datas()
}

// Fields implements the Rows interface.
func (r *BaseRows) Fields() []*querypb.Field {
	return r.fields
}

// Bytes returns all the memory usage which read by this row cursor.
func (r *BaseRows) Bytes() int {
	return r.bytes
}

// RowsAffected implements the Rows interface.
func (r *BaseRows) RowsAffected() uint64 {
	return r.rowsAffected
}

// LastInsertID implements the Rows interface.
func (r *BaseRows) LastInsertID() uint64 {
	return r.insertID
}

// LastError implements the Rows interface.
func (r *BaseRows) LastError() error {
	return r.err
}

// NewTextRows creates TextRows.
func NewTextRows(c Conn) *TextRows {
	textRows := &TextRows{}
	textRows.c = c
	textRows.buffer = common.NewBuffer(8)
	return textRows
}

// NewBinaryRows creates BinaryRows.
func NewBinaryRows(c Conn) *BinaryRows {
	binaryRows := &BinaryRows{}
	binaryRows.c = c
	binaryRows.buffer = common.NewBuffer(8)
	return binaryRows
}

// RowValues implements the Rows interface.
// https://dev.mysql.com/doc/internals/en/binary-protocol-resultset-row.html
func (r *BinaryRows) RowValues() ([]sqltypes.Value, error) {
	if r.fields == nil {
		return nil, errors.New("rows.fields is NIL")
	}

	header, err := r.buffer.ReadU8()
	if err != nil {
		return nil, err
	}
	if header != proto.OK_PACKET {
		return nil, fmt.Errorf("binary.rows.header.is.not.ok[%v]", header)
	}

	colCount := len(r.fields)
	// NULL-bitmap,  [(column-count + 7 + 2) / 8 bytes]
	nullMask, err := r.buffer.ReadBytes(int((colCount + 7 + 2) / 8))
	if err != nil {
		return nil, err
	}

	result := make([]sqltypes.Value, colCount)
	for i := 0; i < colCount; i++ {
		// Field is NULL
		// (byte >> bit-pos) % 2 == 1
		if ((nullMask[(i+2)>>3] >> uint((i+2)&7)) & 1) == 1 {
			result[i] = sqltypes.Value{}
			continue
		}

		v, err := sqltypes.ParseMySQLValues(r.buffer, r.fields[i].Type)
		if err != nil {
			r.c.Cleanup()
			return nil, err
		}

		if v != nil {
			val, err := sqltypes.BuildValue(v)
			if err != nil {
				r.c.Cleanup()
				return nil, err
			}
			r.bytes += val.Len()
			result[i] = val
		} else {
			result[i] = sqltypes.Value{}
		}
	}
	return result, nil
}
