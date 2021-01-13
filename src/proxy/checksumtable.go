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

// handleChecksumTable used to handle the 'CHECKSUM TABLE' command.
func (spanner *Spanner) handleChecksumTable(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	database := session.Schema()
	checksum := node.(*sqlparser.Checksum)
	newqr := &sqltypes.Result{}
	newqr.Fields = []*querypb.Field{
		{Name: "Table", Type: querypb.Type_VARCHAR},
		{Name: "Checksum", Type: querypb.Type_INT64},
	}

	for _, tbl := range checksum.Tables {
		// Construct a new sql with check one table one time, we'll send single table to backends.
		newNode := *checksum
		newNode.Tables = sqlparser.TableNames{tbl}

		// Output format of mysql client: db.tbl
		table := tbl.Name.String()
		schema := tbl.Qualifier.String()
		if schema != "" {
			table = fmt.Sprintf("%v.%v", schema, table)
		} else {
			table = fmt.Sprintf("%v.%v", database, table)
		}

		// If checksum with quick option, set null because innodb does not support "quick" option.
		if newNode.ChecksumOption == sqlparser.ChecksumOptionQuick {
			row := []sqltypes.Value{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(table)),
				sqltypes.MakeTrusted(querypb.Type_INT64, []byte("NULL")),
			}
			newqr.Rows = append(newqr.Rows, row)
			newqr.RowsAffected++
			continue
		}

		qr, err := spanner.ExecuteNormal(session, database, sqlparser.String(&newNode), &newNode)
		if err != nil {
			// Database or table not exist, we return NULL
			if strings.Contains(fmt.Sprintf("%+v", err), "doesn't exist") {
				// Return NULL
				row := []sqltypes.Value{
					sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(table)),
					sqltypes.MakeTrusted(querypb.Type_INT64, []byte("NULL")),
				}
				newqr.Rows = append(newqr.Rows, row)
				newqr.RowsAffected++
				continue
			}
			// error: like "No database selected", just return directly.
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
		newqr.RowsAffected++
	}
	return newqr, nil
}
