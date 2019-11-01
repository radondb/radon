/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

import (
	"router"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
)

func checkTbName(tbInfos map[string]*tableInfo, node sqlparser.SQLNode) error {
	return sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		if col, ok := node.(*sqlparser.ColName); ok {
			tableName := col.Qualifier.Name.String()
			if tableName != "" {
				if _, ok := tbInfos[tableName]; !ok {
					buf := sqlparser.NewTrackedBuffer(nil)
					col.Format(buf)
					return false, errors.Errorf("unsupported: unknown.column.'%s'.in.exprs", buf.String())
				}
			}
		}
		return true, nil
	}, node)
}

// checkTbInNode used to check whether the filter's referTables in the tbInfos.
func checkTbInNode(referTables []string, tbInfos map[string]*tableInfo) bool {
	if len(referTables) == 0 {
		return true
	}
	for _, tb := range referTables {
		if _, ok := tbInfos[tb]; !ok {
			return false
		}
	}
	return true
}

// getIndex used to get index from router.
func getIndex(router *router.Router, tbInfo *tableInfo, val *sqlparser.SQLVal) error {
	idx, err := router.GetIndex(tbInfo.database, tbInfo.tableName, val)
	if err != nil {
		return err
	}

	tbInfo.parent.index = append(tbInfo.parent.index, idx)
	return nil
}

// checkShard used to check whether the col is shardkey.
func checkShard(table, col string, tbInfos map[string]*tableInfo, router *router.Router) (bool, error) {
	tbInfo, ok := tbInfos[table]
	if !ok {
		return false, errors.Errorf("unsupported: unknown.column.'%s.%s'.in.field.list", table, col)
	}

	if tbInfo.shardKey != "" && tbInfo.shardKey == col {
		return true, nil
	}
	return false, nil
}

// getOneTableInfo get a tableInfo.
func getOneTableInfo(tbInfos map[string]*tableInfo) (string, *tableInfo) {
	for tb, tbInfo := range tbInfos {
		return tb, tbInfo
	}
	return "", nil
}

// procure requests for the specified column from the plan
// and returns the join var name for it.
func procure(tbInfos map[string]*tableInfo, col *sqlparser.ColName) string {
	var joinVar string
	field := col.Name.String()
	table := col.Qualifier.Name.String()
	tbInfo := tbInfos[table]
	node := tbInfo.parent
	jn := node.parent.(*JoinNode)

	joinVar = col.Qualifier.Name.CompliantName() + "_" + col.Name.CompliantName()
	if _, ok := jn.Vars[joinVar]; ok {
		return joinVar
	}

	tuples := node.getFields()
	index := -1
	for i, tuple := range tuples {
		if tuple.isCol {
			if field == tuple.field && table == tuple.referTables[0] {
				index = i
				break
			}
		}
	}
	// key not in the select fields.
	if index == -1 {
		tuple := selectTuple{
			expr:        &sqlparser.AliasedExpr{Expr: col},
			field:       field,
			referTables: []string{table},
		}
		index, _ = node.pushSelectExpr(tuple)
	}

	jn.Vars[joinVar] = index
	return joinVar
}
