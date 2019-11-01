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

	"github.com/xelabs/go-mysqlstack/sqlparser"
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
			if shardkey == assignment.Name.Name.String() {
				return true
			}
		}
	}
	return false
}
