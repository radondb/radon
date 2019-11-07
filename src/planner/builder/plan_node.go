/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

import (
	"xcontext"

	"github.com/xelabs/go-mysqlstack/sqlparser"
)

// PlanNode interface.
type PlanNode interface {
	buildQuery(tbInfos map[string]*tableInfo)
	Children() []ChildPlan
	getFields() []selectTuple
	getReferTables() map[string]*tableInfo
	GetQuery() []xcontext.QueryTuple
	pushOrderBy(sel sqlparser.SelectStatement) error
	pushLimit(sel sqlparser.SelectStatement) error
}

// SelectNode interface.
type SelectNode interface {
	PlanNode
	pushFilter(filters []exprInfo) error
	pushKeyFilter(filter exprInfo, table, field string) error
	setParent(p SelectNode)
	setWhereFilter(filter exprInfo)
	setNoTableFilter(exprs []sqlparser.Expr)
	setParenthese(hasParen bool)
	pushEqualCmpr(joins []exprInfo) SelectNode
	calcRoute() (SelectNode, error)
	pushSelectExprs(fields, groups []selectTuple, sel *sqlparser.Select, aggTyp aggrType) error
	pushSelectExpr(field selectTuple) (int, error)
	pushHaving(havings []exprInfo) error
	pushMisc(sel *sqlparser.Select)
	reOrder(int)
	Order() int
}

// findLCA get the two plannode's lowest common ancestors node.
func findLCA(h, p1, p2 SelectNode) SelectNode {
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
