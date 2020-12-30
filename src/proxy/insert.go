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

	"router"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// handleInsert used to handle the insert command.
func (spanner *Spanner) handleInsert(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	database := session.Schema()
	autoincPlug := spanner.plugins.PlugAutoIncrement()

	nodePtr := node.(*sqlparser.Insert)
	if !nodePtr.Table.Qualifier.IsEmpty() {
		database = nodePtr.Table.Qualifier.String()
	}
	table := nodePtr.Table.Name.String()

	methodType, err := spanner.router.PartitionType(database, table)
	if err != nil {
		return nil, err
	}
	// If single or global table, just send sql to backends directly.
	if methodType == router.MethodTypeHash || methodType == router.MethodTypeList {
		// Pre-filled columns after table for insert if node.Columns is nil.
		// For statement "insert into t ... set ...", the columns will never be nil.
		// For statement "insert ... select...", the columns may be nil, but we hasn`t support yet.
		// For statement "insert ... values...", the columns may be nil.
		// e.g.: "insert into t values(...),(...),..."--->"insert into t(c1,c2,c3,...) values(...),(...),...".
		if nodePtr.Columns == nil {
			cfg, err := spanner.router.TableConfig(database, table)
			if err != nil {
				return nil, err
			}
			descQuery := fmt.Sprintf("desc %s.%s", database, cfg.Partitions[0].Table)
			qr, err := spanner.ExecuteOnThisBackend(cfg.Partitions[0].Backend, descQuery)
			if err != nil {
				return nil, err
			}
			for _, row := range qr.Rows {
				nodePtr.Columns = append(nodePtr.Columns, sqlparser.NewColIdent(row[0].ToString()))
			}
		}

		// AutoIncrement plugin process.
		// 1. We should not process if table type is single or global
		// 2. For single or global table, fix bug for add additional auto_increment column when do
		// "insert into t values(1,2,3)", after process will be "insert into t(a) values(1,2,3)", this is not correct.
		if err := autoincPlug.Process(database, node.(*sqlparser.Insert)); err != nil {
			return nil, err
		}
	}

	return spanner.ExecuteDML(session, database, query, node)
}
