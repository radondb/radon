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
	"fmt"

	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// Statement -- stmt struct.
type Statement struct {
	Header      byte // 0x00
	ID          uint32
	ColumnCount uint16
	ParamCount  uint16
	Warnings    uint16
	ParamsType  []int32
	ColumnNames []string

	BindVars map[string]*querypb.BindVariable
}

// UnPackStatementPrepare -- used to unpack the stmt-prepare-response packet.
// https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html
func UnPackStatementPrepare(data []byte) (*Statement, error) {
	var err error
	stmt := &Statement{}
	buf := common.ReadBuffer(data)

	// packet indicator [1 byte]
	if stmt.Header, err = buf.ReadU8(); err != nil {
		return nil, sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "invalid stmt-prepare-response packet header: %v", data)
	}
	if stmt.Header != OK_PACKET {
		return nil, sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "invalid stmt-prepare-response packet header: %v", stmt.Header)
	}

	// Statement id [4 bytes]
	if stmt.ID, err = buf.ReadU32(); err != nil {
		return nil, sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "invalid stmt-prepare-response packet stmt.ID: %v", data)
	}

	// Column count [16 bit uint]
	if stmt.ColumnCount, err = buf.ReadU16(); err != nil {
		return nil, sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "invalid stmt-prepare-response packet column.count: %v", data)
	}

	// Param count [16 bit uint]
	if stmt.ParamCount, err = buf.ReadU16(); err != nil {
		return nil, sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "invalid stmt-prepare-response packet param.count: %v", data)
	}

	// Reserved [8 bit]
	if _, err = buf.ReadU8(); err != nil {
		return nil, sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "invalid stmt-prepare-response packet reserved: %v", data)
	}

	// Warnings [16 bit uint]
	if stmt.Warnings, err = buf.ReadU16(); err != nil {
		return nil, sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "invalid stmt-prepare-response packet warnings: %v", data)
	}
	return stmt, nil
}

// PackStatementPrepare -- used to pack the stmt prepare resp packet.
func PackStatementPrepare(stmt *Statement) []byte {
	buf := common.NewBuffer(64)

	// [00] OK
	buf.WriteU8(OK_PACKET)

	// Statement id [4 bytes]
	buf.WriteU32(stmt.ID)

	// Column count [16 bit uint]
	buf.WriteU16(stmt.ColumnCount)

	// Param count [16 bit uint]
	buf.WriteU16(stmt.ParamCount)

	// reserved_1 (1) -- [00] filler
	buf.WriteZero(1)

	// Warnings [16 bit uint]
	buf.WriteU16(stmt.Warnings)

	return buf.Datas()
}

// PackStatementExecute -- used to pack the stmt execute packet from the client.
// https://dev.mysql.com/doc/internals/en/com-stmt-execute.html
func PackStatementExecute(stmtID uint32, parameters []sqltypes.Value) ([]byte, error) {
	paramsLen := len(parameters)
	nullBitMapLen := (paramsLen + 7) / 8

	nullMask := make([]byte, nullBitMapLen)
	if paramsLen > 0 {
		for i := 0; i < nullBitMapLen; i++ {
			nullMask[i] = 0x00
		}
	}

	var paramsType []byte
	var paramsValue []byte
	for i, param := range parameters {
		// Handle null mask.
		if param.IsNull() {
			nullMask[i/8] |= 1 << (uint(i) & 7)
		} else {
			v, err := param.ToMySQL()
			if err != nil {
				return nil, err
			}
			paramsValue = append(paramsValue, v...)
		}
		typ, flags := sqltypes.TypeToMySQL(param.Type())
		paramsType = append(paramsType, byte(typ))
		paramsType = append(paramsType, byte(flags))
	}

	buf := common.NewBuffer(64)

	// Statement ID[4 bytes]
	buf.WriteU32(stmtID)

	// flags (0: CURSOR_TYPE_NO_CURSOR) [1 byte]
	buf.WriteU8(0x00)

	// iteration_count (uint32(1)) [4 bytes]
	buf.WriteU32(0x01)

	if paramsLen > 0 {
		// NULL-bitmap, length: (num-params+7)/8
		buf.WriteBytes(nullMask)

		// newParameterBoundFlag 1 [1 byte]
		buf.WriteU8(1)

		// params type
		buf.WriteBytes(paramsType)
		// params value
		buf.WriteBytes(paramsValue)
	}
	return buf.Datas(), nil
}

// UnPackStatementExecute -- unpack the stmt-execute packet from client.
func UnPackStatementExecute(data []byte, prepare *Statement, parseValueFn func(*common.Buffer, querypb.Type) (interface{}, error)) error {
	var err error
	bitMap := make([]byte, 0)
	buf := common.ReadBuffer(data)

	if _, err = buf.ReadU32(); err != nil {
		return sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "reading statement ID failed")
	}

	// cursor type flags
	if _, err = buf.ReadU8(); err != nil {
		return sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "reading cursor type flags failed")
	}

	// iteration count
	var itercount uint32
	if itercount, err = buf.ReadU32(); err != nil {
		return sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "reading iteration count failed")
	}
	if itercount != 1 {
		return sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "iteration count is not equal to 1")
	}

	if prepare.ParamCount > 0 {
		if bitMap, err = buf.ReadBytes(int((prepare.ParamCount + 7) / 8)); err != nil {
			return sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "reading NULL-bitmap failed")
		}

		var newParamsBoundFlag byte
		if newParamsBoundFlag, err = buf.ReadU8(); err != nil {
			return sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "reading NULL-bitmap failed")
		}
		if newParamsBoundFlag == 0x01 {
			var mysqlType, flags byte
			for i := uint16(0); i < prepare.ParamCount; i++ {
				if mysqlType, err = buf.ReadU8(); err != nil {
					return sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "reading parameter type failed")
				}

				if flags, err = buf.ReadU8(); err != nil {
					return sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, "reading parameter flags failed")
				}
				// Convert MySQL type to Vitess type.
				valType, err := sqltypes.MySQLToType(int64(mysqlType), int64(flags))
				if err != nil {
					return sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, fmt.Sprintf("MySQLToType(%v,%v) failed: %v", mysqlType, flags, err))
				}
				prepare.ParamsType[i] = int32(valType)
			}
		}

		for i := uint16(0); i < prepare.ParamCount; i++ {
			var val interface{}
			if prepare.ParamsType[i] == int32(sqltypes.Text) || prepare.ParamsType[i] == int32(sqltypes.Blob) {
				continue
			}

			if (bitMap[i/8] & (1 << uint(i%8))) > 0 {
				val, err = parseValueFn(buf, sqltypes.Null)
			} else {
				val, err = parseValueFn(buf, querypb.Type(prepare.ParamsType[i]))
			}
			if err != nil {
				return sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, fmt.Sprintf("decoding parameter value failed(%v) failed: %v", prepare.ParamsType[i], err))
			}

			// If value is nil, must set bind variables to nil.
			bv, err := sqltypes.BuildBindVariable(val)
			if err != nil {
				return sqldb.NewSQLErrorf(sqldb.ER_MALFORMED_PACKET, fmt.Sprintf("build converted parameters value failed: %v", err))
			}
			prepare.BindVars[fmt.Sprintf("v%d", i+1)] = bv
		}
	}
	return nil
}
