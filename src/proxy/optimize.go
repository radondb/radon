/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"sort"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// handleOptimizeTable used to handle the 'Optimize TABLE ...' command.
// +--------------+----------+----------+-------------------------------------------------------------------+
// | Table        | Op       | Msg_type | Msg_text                                                          |
// +--------------+----------+----------+-------------------------------------------------------------------+
// | test.t       | optimize | note     | Table does not support optimize, doing recreate + analyze instead |
// | test.t       | optimize | status   | OK                                                                |
// | test.t1_0001 | optimize | status   | OK                                                                |
// | test.t1_0001 | optimize | note     | Table does not support optimize, doing recreate + analyze instead |
// +--------------+----------+----------+-------------------------------------------------------------------+
func (spanner *Spanner) handleOptimizeTable(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	database := session.Schema()
	optimize := node.(*sqlparser.Optimize)
	newqr := &sqltypes.Result{}

	for _, tbl := range optimize.Tables {
		// Construct a new sql with check one table one time, we'll send single table to backends.
		newNode := *optimize
		newNode.Tables = sqlparser.TableNames{tbl}
		qr, err := spanner.ExecuteNormal(session, database, sqlparser.String(&newNode), &newNode)
		if err != nil {
			return nil, err
		}
		newqr.AppendResult(qr)
	}

	// 1. sort by field "Table"
	sort.Slice(newqr.Rows, func(i, j int) bool {
		val := sqltypes.NullsafeCompare(newqr.Rows[i][0], newqr.Rows[j][0])
		return (-1 == val)
	})
	// 2. Formate output to mysql client, note is always displayed first. e.g.:
	// change:
	// | test.t       | optimize | note     | Table does not support optimize, doing recreate + analyze instead |
	// | test.t       | optimize | status   | OK                                                                |
	// to:
	// | test.t       | optimize | status   | OK                                                                |
	// | test.t       | optimize | note     | Table does not support optimize, doing recreate + analyze instead |

	for i := 0; i < len(newqr.Rows); i += 2 {
		j := i + 1
		if -1 == sqltypes.NullsafeCompare(newqr.Rows[i][2], newqr.Rows[j][2]) {
			newqr.Rows[i][2], newqr.Rows[j][2] = newqr.Rows[j][2], newqr.Rows[i][2]
			newqr.Rows[i][3], newqr.Rows[j][3] = newqr.Rows[j][3], newqr.Rows[i][3]
		}
	}
	return newqr, nil
}
