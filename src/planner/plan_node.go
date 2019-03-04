/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"github.com/xelabs/go-mysqlstack/sqlparser"
)

// PlanNode interface.
type PlanNode interface {
	getReferredTables() map[string]*TableInfo
	setParenthese(hasParen bool)
	pushFilter(filters []filterTuple) error
	setParent(p PlanNode)
	setWhereFilter(filter sqlparser.Expr)
	setNoTableFilter(exprs []sqlparser.Expr)
	pushJoinInWhere(joins []joinTuple) (PlanNode, error)
	calcRoute() (PlanNode, error)
	spliceWhere() error
	pushSelectExprs(fileds, groups []selectTuple, sel *sqlparser.Select, hasAggregates bool) error
	pushHaving(havings []filterTuple) error
	pushOrderBy(sel *sqlparser.Select, fileds []selectTuple) error
	pushLimit(sel *sqlparser.Select) error
	pushMisc(sel *sqlparser.Select)
	Children() *PlanTree
}

// findLCA get the two plannode's lowest common ancestors node.
func findLCA(h, p1, p2 PlanNode) PlanNode {
	if p1 == h || p2 == h {
		return h
	}
	jn, ok := h.(*JoinNode)
	if !ok {
		return nil
	}
	pl := findLCA(jn.Left, p1, p2)
	pr := findLCA(jn.Right, p1, p2)

	if pl != nil && pr != nil {
		return jn
	}
	if pl == nil {
		return pr
	}
	return pl
}

// getOneTableInfo get a tableInfo.
func getOneTableInfo(tbInfos map[string]*TableInfo) (string, *TableInfo) {
	for tb, tbInfo := range tbInfos {
		return tb, tbInfo
	}
	return "", nil
}
