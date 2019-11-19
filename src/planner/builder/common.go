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

func isContainKey(a []string, b string) bool {
	for _, c := range a {
		if c == b {
			return true
		}
	}
	return false
}

// fetchIndex used to fetch index from router.
func fetchIndex(tbInfo *tableInfo, val *sqlparser.SQLVal, router *router.Router) error {
	idx, err := router.GetIndex(tbInfo.database, tbInfo.tableName, val)
	if err != nil {
		return err
	}

	tbInfo.parent.indexes = append(tbInfo.parent.indexes, idx)
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
