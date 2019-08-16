/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"fmt"
	"strings"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// handleChecksumTable used to handle the 'CHECKSUM TABLE ' command.
func (spanner *Spanner) handleChecksumTable(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	database := session.Schema()
	ast := node.(*sqlparser.Checksum)
	table := ast.Table.Name.String()
	schema := ast.Table.Qualifier.String()

	// Format: db.tbl
	if schema != "" {
		table = fmt.Sprintf("%v.%v", schema, table)
	} else {
		table = fmt.Sprintf("%v.%v", database, table)
	}

	newqr := &sqltypes.Result{}
	newqr.RowsAffected = 1
	newqr.Fields = []*querypb.Field{
		{Name: "Table", Type: querypb.Type_VARCHAR},
		{Name: "Checksum", Type: querypb.Type_INT64},
	}

	qr, err := spanner.ExecuteNormal(session, database, query, ast)
	if err != nil {
		// Database or table not exist, we return NULL
		if strings.Contains(fmt.Sprintf("%+v", err), "doesn't exist") {
			// Return NULL
			row := []sqltypes.Value{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(table)),
				sqltypes.MakeTrusted(querypb.Type_INT64, []byte("NULL")),
			}
			newqr.Rows = append(newqr.Rows, row)
			return newqr, nil
		}
		return nil, err
	}

	// Merge checksum.
	var crc uint32
	for _, row := range qr.Rows {
		crc += uint32((row[1].ToNative().(int64)))
	}

	row := []sqltypes.Value{
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(table)),
		sqltypes.MakeTrusted(querypb.Type_INT64, []byte(fmt.Sprintf("%v", crc))),
	}
	newqr.Rows = append(newqr.Rows, row)
	return newqr, nil
}
