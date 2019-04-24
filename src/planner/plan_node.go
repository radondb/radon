/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"xcontext"

	"github.com/xelabs/go-mysqlstack/sqlparser"
)

// PlanNode interface.
type PlanNode interface {
	getReferredTables() map[string]*TableInfo
	getFields() []selectTuple
	setParenthese(hasParen bool)
	pushFilter(filters []filterTuple) error
	setParent(p PlanNode)
	setWhereFilter(filter filterTuple)
	setNoTableFilter(exprs []sqlparser.Expr)
	pushEqualCmpr(joins []joinTuple) PlanNode
	calcRoute() (PlanNode, error)
	pushSelectExprs(fields, groups []selectTuple, sel *sqlparser.Select, hasAggregates bool) error
	pushSelectExpr(field selectTuple) (int, error)
	pushHaving(havings []filterTuple) error
	pushOrderBy(sel *sqlparser.Select, fields []selectTuple) error
	pushLimit(sel *sqlparser.Select) error
	pushMisc(sel *sqlparser.Select)
	Children() *PlanTree
	buildQuery(tbInfos map[string]*TableInfo)
	GetQuery() []xcontext.QueryTuple
	reOrder(int)
	Order() int
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

// procure requests for the specified column from the plan
// and returns the join var name for it.
func procure(tbInfos map[string]*TableInfo, col *sqlparser.ColName) string {
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
