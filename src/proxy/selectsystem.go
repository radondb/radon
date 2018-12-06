/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"strings"

	"github.com/xelabs/go-mysqlstack/common"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// handleSysSelect used to handle the system database(information_schema etc.) select command.
func (spanner *Spanner) handleSelectSystem(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	log := spanner.log
	ast := node.(*sqlparser.Select)

	aliasTableExpr := ast.From[0].(*sqlparser.AliasedTableExpr)
	tb, _ := aliasTableExpr.Expr.(sqlparser.TableName)
	database := tb.Qualifier.String()
	table := tb.Name.String()
	log.Debug("select.system:table:%v, db:%v", table, database)

	switch strings.ToUpper(database) {
	case "INFORMATION_SCHEMA":
		return spanner.handleSelectInformationschema(query, table, ast)
	}
	return spanner.ExecuteSingle(query)
}

// handleSelectInformationschema -- used to handle the INFORMATION_SCHEMA query.
// If the query is:
// > select * from information_schema.COLUMNS where TABLE_NAME='t1' and TABLE_SCHEMA='test'
// The TABLE_NAME value must be replaced by the partition table:
// > select * from information_schema.COLUMNS where TABLE_NAME='t1_0000' and TABLE_SCHEMA='test'
func (spanner *Spanner) handleSelectInformationschema(query string, tbl string, node *sqlparser.Select) (*sqltypes.Result, error) {
	router := spanner.router

	if strings.EqualFold(tbl, "COLUMNS") {
		if node.Where != nil {
			var tblName *sqlparser.SQLVal
			var tblSchema *sqlparser.SQLVal

			_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
				if comparison, ok := node.(*sqlparser.ComparisonExpr); ok {
					switch comparison.Operator {
					case sqlparser.EqualStr:
						colname, ok := comparison.Left.(*sqlparser.ColName)
						if ok {
							if strings.EqualFold(colname.Name.String(), "TABLE_NAME") {
								tblName, _ = comparison.Right.(*sqlparser.SQLVal)
							}

							if strings.EqualFold(colname.Name.String(), "TABLE_SCHEMA") {
								tblSchema, _ = comparison.Right.(*sqlparser.SQLVal)
							}
						}
					}
				}
				return true, nil
			}, node.Where)

			if tblName != nil && tblSchema != nil {
				name := common.BytesToString(tblName.Val)
				schema := common.BytesToString(tblSchema.Val)

				// Get one partition table from the router.
				parts, err := router.Lookup(schema, name, nil, nil)
				if err != nil {
					return nil, err
				}
				partTable := parts[0].Table
				backend := parts[0].Backend

				// Replace TABLE_NAME value to partition table.
				_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
					if comparison, ok := node.(*sqlparser.ComparisonExpr); ok {
						switch comparison.Operator {
						case sqlparser.EqualStr:
							colname, ok := comparison.Left.(*sqlparser.ColName)
							if ok {
								if strings.EqualFold(colname.Name.String(), "TABLE_NAME") {
									comparison.Right = sqlparser.NewStrVal([]byte(partTable))
									return false, nil
								}
							}
						}
					}
					return true, nil
				}, node.Where)

				// The final sql.
				sqlbuf := sqlparser.NewTrackedBuffer(nil)
				node.Format(sqlbuf)
				rewritten := sqlbuf.String()
				spanner.log.Debug("---:%s", rewritten)
				return spanner.ExecuteOnThisBackend(backend, rewritten)
			}
		}
	}
	return spanner.ExecuteSingle(query)
}
