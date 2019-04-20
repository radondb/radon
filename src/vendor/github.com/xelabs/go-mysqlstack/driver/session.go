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
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/xelabs/go-mysqlstack/packet"
	"github.com/xelabs/go-mysqlstack/proto"
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/xlog"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// Session is a client connection with greeting and auth.
type Session struct {
	id            uint32
	mu            sync.RWMutex
	log           *xlog.Log
	conn          net.Conn
	schema        string
	auth          *proto.Auth
	packets       *packet.Packets
	greeting      *proto.Greeting
	lastQueryTime time.Time
	statementID   uint32                // used to identify different statements for the same session.
	statements    map[uint32]*Statement // Save the metadata of the session related to the prepare operation.
}

func newSession(log *xlog.Log, ID uint32, serverVersion string, conn net.Conn) *Session {
	return &Session{
		id:            ID,
		log:           log,
		conn:          conn,
		auth:          proto.NewAuth(),
		greeting:      proto.NewGreeting(ID, serverVersion),
		packets:       packet.NewPackets(conn),
		lastQueryTime: time.Now(),
		statements:    make(map[uint32]*Statement),
	}
}

func (s *Session) writeErrFromError(err error) error {
	if se, ok := err.(*sqldb.SQLError); ok {
		return s.packets.WriteERR(se.Num, se.State, "%v", se.Message)
	}
	unknow := sqldb.NewSQLErrorf(sqldb.ER_UNKNOWN_ERROR, "%v", err)
	return s.packets.WriteERR(unknow.Num, unknow.State, unknow.Message)
}

func (s *Session) writeFields(result *sqltypes.Result) error {
	// 1. Write columns.
	if err := s.packets.AppendColumns(result.Fields); err != nil {
		return err
	}

	if (s.auth.ClientFlags() & sqldb.CLIENT_DEPRECATE_EOF) == 0 {
		if err := s.packets.AppendEOF(s.greeting.Status(), result.Warnings); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) appendTextRows(result *sqltypes.Result) error {
	// 2. Append rows.
	for _, row := range result.Rows {
		rowBuf := common.NewBuffer(16)
		for _, val := range row {
			if val.IsNull() {
				rowBuf.WriteLenEncodeNUL()
			} else {
				rowBuf.WriteLenEncodeBytes(val.Raw())
			}
		}
		if err := s.packets.Append(rowBuf.Datas()); err != nil {
			return err
		}
	}
	return nil
}

// http://dev.mysql.com/doc/internals/en/binary-protocol-resultset-row.html
func (s *Session) appendBinaryRows(result *sqltypes.Result) error {
	colCount := len(result.Fields)

	for _, row := range result.Rows {
		valBuf := common.NewBuffer(16)
		nullMask := make([]byte, (colCount+7+2)/8)

		for fieldPos, val := range row {
			if val.IsNull() || (val.Raw() == nil) {
				bytePos := (fieldPos + 2) / 8
				bitPos := uint8((fieldPos + 2) % 8)
				//doc: https://dev.mysql.com/doc/internals/en/null-bitmap.html
				//nulls[byte_pos] |= 1 << bit_pos
				//nulls[1] |= 1 << 2;
				nullMask[bytePos] |= 1 << bitPos
				continue
			}

			v, err := val.ToMySQL()
			if err != nil {
				return err
			}
			valBuf.WriteBytes(v)
		}

		rowBuf := common.NewBuffer(16)
		// OK header.
		rowBuf.WriteU8(proto.OK_PACKET)
		// NULL-bitmap
		rowBuf.WriteBytes(nullMask)
		rowBuf.WriteBytes(valBuf.Datas())
		if err := s.packets.Append(rowBuf.Datas()); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) writeFinish(result *sqltypes.Result) error {
	// 3. Write EOF.
	if (s.auth.ClientFlags() & sqldb.CLIENT_DEPRECATE_EOF) == 0 {
		if err := s.packets.AppendEOF(s.greeting.Status(), result.Warnings); err != nil {
			return err
		}
	} else {
		if err := s.packets.AppendOKWithEOFHeader(result.RowsAffected, result.InsertID, s.greeting.Status(), result.Warnings); err != nil {
			return err
		}
	}
	return nil
}

func (s *Session) flush() error {
	// 4. Write to stream.
	return s.packets.Flush()
}

func (s *Session) writeBaseRows(rowMode RowMode, result *sqltypes.Result) error {
	if len(result.Fields) == 0 {
		if result.State == sqltypes.RStateNone {
			// This is just an INSERT result, send an OK packet.
			return s.packets.WriteOK(result.RowsAffected, result.InsertID, s.greeting.Status(), result.Warnings)
		}
		return fmt.Errorf("unexpected: result.without.no.fields.but.has.rows.result:%+v", result)
	}

	switch result.State {
	case sqltypes.RStateNone:
		if err := s.writeFields(result); err != nil {
			return err
		}
		switch rowMode {
		case TextRowMode:
			if err := s.appendTextRows(result); err != nil {
				return err
			}
		case BinaryRowMode:
			if err := s.appendBinaryRows(result); err != nil {
				return err
			}
		}
		if err := s.writeFinish(result); err != nil {
			return err
		}
	case sqltypes.RStateFields:
		if err := s.writeFields(result); err != nil {
			return err
		}
	case sqltypes.RStateRows:
		switch rowMode {
		case TextRowMode:
			if err := s.appendTextRows(result); err != nil {
				return err
			}
		case BinaryRowMode:
			if err := s.appendBinaryRows(result); err != nil {
				return err
			}
		}
	case sqltypes.RStateFinished:
		if err := s.writeFinish(result); err != nil {
			return err
		}
	}
	return s.flush()
}

func (s *Session) writeTextRows(result *sqltypes.Result) error {
	return s.writeBaseRows(TextRowMode, result)
}

func (s *Session) writeBinaryRows(result *sqltypes.Result) error {
	return s.writeBaseRows(BinaryRowMode, result)
}

// writeStatementPrepareResult -- writes the packed prepare result to client.
func (s *Session) writeStatementPrepareResult(stmt *Statement) error {
	protoStmt := &proto.Statement{
		ID:         stmt.ID,
		ParamCount: stmt.ParamCount,
	}
	if err := s.packets.WriteStatementPrepareResponse(s.auth.ClientFlags(), protoStmt); err != nil {
		return err
	}
	return s.flush()
}

// Close used to close the connection.
func (s *Session) Close() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
}

// ID returns the connection ID.
func (s *Session) ID() uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.id
}

// Addr returns the remote address.
func (s *Session) Addr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.conn != nil {
		return s.conn.RemoteAddr().String()
	}
	return "unknow"
}

// SetSchema used to set the schema.
func (s *Session) SetSchema(schema string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schema = schema
}

// Schema returns the schema.
func (s *Session) Schema() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.schema
}

// User returns the user of auth.
func (s *Session) User() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.auth.User()
}

// Salt returns the salt of greeting.
func (s *Session) Salt() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.greeting.Salt
}

// Scramble returns the scramble of auth.
func (s *Session) Scramble() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.auth.AuthResponse()
}

// Charset returns the charset of auth.
func (s *Session) Charset() uint8 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.auth.Charset()
}

// LastQueryTime returns the lastQueryTime.
func (s *Session) LastQueryTime() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastQueryTime
}

// updateLastQueryTime update the lastQueryTime.
func (s *Session) updateLastQueryTime(time time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastQueryTime = time
}
