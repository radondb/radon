/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"errors"
	"fmt"

	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func hasSubquery(node sqlparser.SQLNode) bool {
	has := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		if _, ok := node.(*sqlparser.Subquery); ok {
			has = true
			return false, errors.New("dummy")
		}
		return true, nil
	}, node)
	return has
}

// isUpdateShardKey returns true if any of the update
// expressions modify a shardkey column.
func isUpdateShardKey(exprs sqlparser.UpdateExprs, shardkey string) bool {
	if shardkey != "" {
		for _, assignment := range exprs {
			if assignment.Name.Name.EqualString(shardkey) {
				return true
			}
		}
	}
	return false
}

// checkField is used to check if the field's Qualifier is illegal or not in where or order by clause.
func checkField(database, table string, node sqlparser.SQLNode, log *xlog.Log) error {
	switch node.(type) {
	case *sqlparser.Delete:
		// Here we not should do check on limit clause, leave it to the backend mysql.
		nodePtr := node.(*sqlparser.Delete)
		if err := checkFieldImpl(database, table, nodePtr.Where, "where clause"); err != nil {
			return err
		}
		if err := checkFieldImpl(database, table, nodePtr.OrderBy, "order clause"); err != nil {
			return err
		}
	default:
		log.Warning("currently.check.field.only.support.delete.statement.")
	}
	return nil
}

// checkFieldImpl is an implementation of checkField() function.
func checkFieldImpl(database, table string, node sqlparser.SQLNode, suffix string) error {
	err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node.(type) {
		case *sqlparser.ColName:
			nodePtr := node.(*sqlparser.ColName)
			colDB := nodePtr.Qualifier.Qualifier.String()
			colTbl := nodePtr.Qualifier.Name.String()
			colName := nodePtr.Name.String()

			if colDB != "" && colTbl != "" {
				// case 1: where/order by db.t.a
				if colDB != database || colTbl != table {
					badField := fmt.Sprintf("%s.%s.%s", colDB, colTbl, colName)
					return false, sqldb.NewSQLError(sqldb.ER_BAD_FIELD_ERROR, badField, suffix)
				}
			} else if colDB == "" && colTbl != "" {
				// case 2: where/order by t.a
				if colTbl != table {
					badField := fmt.Sprintf("%s.%s", colTbl, colName)
					return false, sqldb.NewSQLError(sqldb.ER_BAD_FIELD_ERROR, badField, suffix)
				}
			}
			// case 3: where/order by a, return
			// Currently if column a is not in table t, the err msg output is different with mysql.
			// e.g.: DELETE FROM integrate_test.t WHERE post='1';
			// In radondb we'll get:
			// ERROR 1054 (42S22): Unknown column 'integrate_test.t_0032.post' in 'where clause'
			// But in mysql we get:
			// ERROR 1054 (42S22): Unknown column 'post' in 'where clause'
			return true, nil
		default:
			// If node is not sqlparser.ColName type, return true and continue visit.
			return true, nil
		}
	}, node)
	return err
}

// rewriteField used to rewrite column field in where/order by clause.
// Currently only used by delete statement.
func rewriteField(database, partTable string, newNode sqlparser.SQLNode, log *xlog.Log) {
	switch newNode.(type) {
	case *sqlparser.Delete:
		// Here we should not do rewrite on limit clause, leave it to the backend mysql.
		nodePtr := newNode.(*sqlparser.Delete)
		rewriteFieldImpl(database, partTable, nodePtr.Where, nodePtr.OrderBy)
	default:
		log.Warning("currently.rewrite.field.only.support.delete.statement.")
	}
}

// rewriteFieldImpl is an implementation of rewriteField() function
func rewriteFieldImpl(database, partTable string, newNodes ...sqlparser.SQLNode) {
	sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node.(type) {
		case *sqlparser.ColName:
			nodePtr := node.(*sqlparser.ColName)
			nodePtr.Qualifier.Name = sqlparser.NewTableIdent(partTable)
			nodePtr.Qualifier.Qualifier = sqlparser.NewTableIdent(database)
			return true, nil
		default:
			// If newNode is not sqlparser.ColName type, return true and continue visit.
			return true, nil
		}
	}, newNodes...)
}
