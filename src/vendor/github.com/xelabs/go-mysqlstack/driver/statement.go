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
	"github.com/xelabs/go-mysqlstack/proto"
	"github.com/xelabs/go-mysqlstack/sqldb"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// Statement --
type Statement struct {
	conn        *conn
	ID          uint32
	ParamCount  uint16
	PrepareStmt string
	ParamsType  []int32
	ColumnNames []string
	BindVars    map[string]*querypb.BindVariable
}

// ComStatementExecute -- statement execute write.
func (s *Statement) ComStatementExecute(parameters []sqltypes.Value) error {
	var err error
	var datas []byte
	var iRows Rows

	if datas, err = proto.PackStatementExecute(s.ID, parameters); err != nil {
		return err
	}

	if iRows, err = s.conn.stmtQuery(sqldb.COM_STMT_EXECUTE, datas); err != nil {
		return err
	}
	for iRows.Next() {
		if _, err := iRows.RowValues(); err != nil {
			s.conn.Cleanup()
			return err
		}
	}
	// Drain the results and check last error.
	if err := iRows.Close(); err != nil {
		s.conn.Cleanup()
		return err
	}
	return nil
}

// ComStatementExecute -- statement execute write.
func (s *Statement) ComStatementQuery(parameters []sqltypes.Value) (*sqltypes.Result, error) {
	var err error
	var datas []byte
	var iRows Rows
	var qrRow []sqltypes.Value
	var qrRows [][]sqltypes.Value

	if datas, err = proto.PackStatementExecute(s.ID, parameters); err != nil {
		return nil, err
	}

	if iRows, err = s.conn.stmtQuery(sqldb.COM_STMT_EXECUTE, datas); err != nil {
		return nil, err
	}
	for iRows.Next() {
		if qrRow, err = iRows.RowValues(); err != nil {
			s.conn.Cleanup()
			return nil, err
		}
		if qrRow != nil {
			qrRows = append(qrRows, qrRow)
		}
	}
	// Drain the results and check last error.
	if err := iRows.Close(); err != nil {
		s.conn.Cleanup()
		return nil, err
	}

	rowsAffected := iRows.RowsAffected()
	if rowsAffected == 0 {
		rowsAffected = uint64(len(qrRows))
	}
	qr := &sqltypes.Result{
		Fields:       iRows.Fields(),
		RowsAffected: rowsAffected,
		InsertID:     iRows.LastInsertID(),
		Rows:         qrRows,
	}
	return qr, err
}

// ComStatementReset -- reset the stmt.
func (s *Statement) ComStatementReset() error {
	var data [4]byte

	// Add arg [32 bit]
	data[0] = byte(s.ID)
	data[1] = byte(s.ID >> 8)
	data[2] = byte(s.ID >> 16)
	data[3] = byte(s.ID >> 24)
	if err := s.conn.packets.WriteCommand(sqldb.COM_STMT_RESET, data[:]); err != nil {
		return err
	}
	return s.conn.packets.ReadOK()
}

// ComStatementClose -- close the stmt.
func (s *Statement) ComStatementClose() error {
	var data [4]byte

	// Add arg [32 bit]
	data[0] = byte(s.ID)
	data[1] = byte(s.ID >> 8)
	data[2] = byte(s.ID >> 16)
	data[3] = byte(s.ID >> 24)
	if err := s.conn.packets.WriteCommand(sqldb.COM_STMT_CLOSE, data[:]); err != nil {
		return err
	}
	return nil
}
