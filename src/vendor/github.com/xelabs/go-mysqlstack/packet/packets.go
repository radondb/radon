/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package packet

import (
	"fmt"
	"net"

	"github.com/xelabs/go-mysqlstack/proto"
	"github.com/xelabs/go-mysqlstack/sqldb"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

const (
	// PACKET_MAX_SIZE used for the max packet size.
	PACKET_MAX_SIZE = (1<<24 - 1) // (16MB - 1）
)

// Packet presents the packet tuple.
type Packet struct {
	SequenceID byte
	Datas      []byte
}

// Packets presents the stream tuple.
type Packets struct {
	seq    uint8
	stream *Stream
}

// NewPackets creates the new packets.
func NewPackets(c net.Conn) *Packets {
	return &Packets{
		stream: NewStream(c, PACKET_MAX_SIZE),
	}
}

// Next used to read the next packet.
func (p *Packets) Next() ([]byte, error) {
	pkt, err := p.stream.Read()
	if err != nil {
		return nil, err
	}

	if pkt.SequenceID != p.seq {
		return nil, sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "pkt.read.seq[%v]!=pkt.actual.seq[%v]", pkt.SequenceID, p.seq)
	}
	p.seq++
	return pkt.Datas, nil
}

// Write writes the packet to the wire.
// It packed as:
// [header]
// [payload]
func (p *Packets) Write(payload []byte) error {
	payLen := len(payload)
	pkt := common.NewBuffer(64)

	// body length(24bits)
	pkt.WriteU24(uint32(payLen))

	// SequenceID
	pkt.WriteU8(p.seq)

	// body
	pkt.WriteBytes(payload)
	if err := p.stream.Write(pkt.Datas()); err != nil {
		return err
	}
	p.seq++
	return nil
}

// WriteCommand writes a command packet to the wire.
func (p *Packets) WriteCommand(command byte, payload []byte) error {
	// reset packet sequence
	p.seq = 0
	pkt := common.NewBuffer(64)

	// body length(24bits):
	// command length + payload length
	payLen := len(payload)
	pkt.WriteU24(uint32(1 + payLen))

	// SequenceID
	pkt.WriteU8(p.seq)

	// command
	pkt.WriteU8(command)

	// body
	pkt.WriteBytes(payload)
	if err := p.stream.Write(pkt.Datas()); err != nil {
		return err
	}
	p.seq++
	return nil
}

// ResetSeq reset sequence to zero.
func (p *Packets) ResetSeq() {
	p.seq = 0
}

// ParseOK used to parse the OK packet.
func (p *Packets) ParseOK(data []byte) (*proto.OK, error) {
	return proto.UnPackOK(data)
}

// WriteOK writes OK packet to the wire.
func (p *Packets) WriteOK(affectedRows, lastInsertID uint64, flags uint16, warnings uint16) error {
	ok := &proto.OK{
		AffectedRows: affectedRows,
		LastInsertID: lastInsertID,
		StatusFlags:  flags,
		Warnings:     warnings,
	}
	return p.Write(proto.PackOK(ok))
}

// ParseERR used to parse the ERR packet.
func (p *Packets) ParseERR(data []byte) error {
	return proto.UnPackERR(data)
}

// WriteERR writes ERR packet to the wire.
func (p *Packets) WriteERR(errorCode uint16, sqlState string, format string, args ...interface{}) error {
	e := &proto.ERR{
		ErrorCode:    errorCode,
		SQLState:     sqlState,
		ErrorMessage: fmt.Sprintf(format, args...),
	}
	return p.Write(proto.PackERR(e))
}

// Append appends packets to buffer but not write to stream.
// This is underlying packet unit.
// NOTICE: SequenceID++
func (p *Packets) Append(rawdata []byte) error {
	pkt := common.NewBuffer(64)

	// body length(24bits):
	// payload length
	pkt.WriteU24(uint32(len(rawdata)))

	// SequenceID
	pkt.WriteU8(p.seq)

	// body
	pkt.WriteBytes(rawdata)
	if err := p.stream.Append(pkt.Datas()); err != nil {
		return err
	}
	p.seq++
	return nil
}

// ReadOK used to read the OK packet.
func (p *Packets) ReadOK() error {
	// EOF packet
	data, err := p.Next()
	if err != nil {
		return err
	}
	switch data[0] {
	case proto.OK_PACKET:
		return nil
	case proto.ERR_PACKET:
		return p.ParseERR(data)
	default:
		return sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "unexpected.ok.packet[%+v]", data)
	}
}

// ReadEOF used to read the EOF packet.
func (p *Packets) ReadEOF() error {
	// EOF packet
	data, err := p.Next()
	if err != nil {
		return err
	}
	switch data[0] {
	case proto.EOF_PACKET:
		return nil
	case proto.ERR_PACKET:
		return p.ParseERR(data)
	default:
		return sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "unexpected.eof.packet[%+v]", data)
	}
}

// AppendEOF appends EOF packet to the stream buffer.
func (p *Packets) AppendEOF(flags uint16, warnings uint16) error {
	eof := &proto.EOF{
		StatusFlags: flags,
		Warnings:    warnings,
	}
	return p.Append(proto.PackEOF(eof))
}

// AppendOKWithEOFHeader appends OK packet to the stream buffer with EOF header.
func (p *Packets) AppendOKWithEOFHeader(affectedRows, lastInsertID uint64, flags uint16, warnings uint16) error {
	ok := &proto.OK{
		AffectedRows: affectedRows,
		LastInsertID: lastInsertID,
		StatusFlags:  flags,
		Warnings:     warnings,
	}
	buf := common.NewBuffer(64)
	buf.WriteU8(proto.EOF_PACKET)
	buf.WriteBytes(proto.PackOK(ok))
	return p.Append(buf.Datas())
}

// AppendColumns used to append column to columns.
func (p *Packets) AppendColumns(columns []*querypb.Field) error {
	// column count
	count := len(columns)
	buf := common.NewBuffer(64)
	buf.WriteLenEncode(uint64(count))
	if err := p.Append(buf.Datas()); err != nil {
		return err
	}

	// columns info
	for i := 0; i < count; i++ {
		buf := common.NewBuffer(64)
		buf.WriteBytes(proto.PackColumn(columns[i]))
		if err := p.Append(buf.Datas()); err != nil {
			return err
		}
	}
	return nil
}

// Flush writes all append-packets to the wire.
func (p *Packets) Flush() error {
	return p.stream.Flush()
}

// ReadComQueryResponse used to read query command response and parse the column count.
// http://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset
// Returns:
// ok, colNumbs, myerr, err
//
// myerr is the error who was send by MySQL server, the client does not close the connection.
// if err is not nil, we(the client) will close the connection.
func (p *Packets) ReadComQueryResponse() (*proto.OK, int, error, error) {
	var err error
	var data []byte
	var numbers uint64

	if data, err = p.Next(); err != nil {
		return nil, 0, nil, err
	}

	ok := &proto.OK{}
	switch data[0] {
	case proto.OK_PACKET:
		// OK.
		if ok, err = p.ParseOK(data); err != nil {
			return nil, 0, nil, err
		}
		return ok, 0, nil, nil
	case proto.ERR_PACKET:
		return nil, 0, p.ParseERR(data), nil
	case 0xfb:
		// Local infile
		return nil, 0, sqldb.NewSQLError(sqldb.ER_UNKNOWN_ERROR, "Local.infile.not.implemented"), nil
	}
	// column count
	if numbers, err = proto.ColumnCount(data); err != nil {
		return nil, 0, nil, err
	}
	return ok, int(numbers), nil, nil
}

// ReadColumns used to read all columns from the stream buffer.
func (p *Packets) ReadColumns(colNumber int) ([]*querypb.Field, error) {
	var err error
	var data []byte

	// column info
	columns := make([]*querypb.Field, 0, colNumber)
	for i := 0; i < colNumber; i++ {
		if data, err = p.Next(); err != nil {
			return nil, err
		}
		column, err := proto.UnpackColumn(data)
		if err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}
	return columns, nil
}

// WriteStatementPrepareResponse -- write the stmt prepare response to client by server.
func (p *Packets) WriteStatementPrepareResponse(clientFlags uint32, stmt *proto.Statement) error {
	// First write statement prepare package.
	datas := proto.PackStatementPrepare(stmt)
	if err := p.Append(datas); err != nil {
		return err
	}

	// Send param fields.
	if stmt.ParamCount > 0 {
		for i := uint16(0); i < stmt.ParamCount; i++ {
			buf := common.NewBuffer(64)
			field := &querypb.Field{Name: "?", Type: sqltypes.VarBinary, Charset: 63}
			buf.WriteBytes(proto.PackColumn(field))
			if err := p.Append(buf.Datas()); err != nil {
				return err
			}
		}
		if (clientFlags & sqldb.CLIENT_DEPRECATE_EOF) == 0 {
			p.AppendEOF(0, 0)
		}
	}
	return p.Flush()
}

// ReadStatementPrepareResponse -- read the stmt prepare response by client from the server.
func (p *Packets) ReadStatementPrepareResponse(clientFlags uint32) (*proto.Statement, error) {
	var err error
	var data []byte

	if data, err = p.Next(); err != nil {
		return nil, err
	}

	switch data[0] {
	case proto.ERR_PACKET:
		return nil, p.ParseERR(data)
	}

	stmt, err := proto.UnPackStatementPrepare(data)
	if err != nil {
		return nil, err
	}

	if stmt.ParamCount > 0 {
		for i := uint16(0); i < stmt.ParamCount; i++ {
			if data, err = p.Next(); err != nil {
				return nil, err
			}
			if _, err = proto.UnpackColumn(data); err != nil {
				return nil, err
			}
		}

		if (clientFlags & sqldb.CLIENT_DEPRECATE_EOF) == 0 {
			if err = p.ReadEOF(); err != nil {
				return nil, err
			}
		}
	}

	if stmt.ColumnCount > 0 {
		for i := uint16(0); i < stmt.ColumnCount; i++ {
			if data, err = p.Next(); err != nil {
				return nil, err
			}
			column, err := proto.UnpackColumn(data)
			if err != nil {
				return nil, err
			}
			stmt.ColumnNames = append(stmt.ColumnNames, column.Name)
		}

		if (clientFlags & sqldb.CLIENT_DEPRECATE_EOF) == 0 {
			if err = p.ReadEOF(); err != nil {
				return nil, err
			}
		}
	}
	return stmt, nil
}
