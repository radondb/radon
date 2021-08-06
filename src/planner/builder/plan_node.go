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
	addNoTableFilter(exprs []sqlparser.Expr)
	buildQuery(root PlanNode)
	calcRoute() (PlanNode, error)
	Children() []ChildPlan
	getFields() []selectTuple
	getReferTables() map[string]*tableInfo
	GetQuery() []xcontext.QueryTuple
	pushSelectExprs(fields, groups []selectTuple, sel *sqlparser.Select, aggTyp aggrType) error
	pushSelectExpr(field selectTuple) (int, error)
	pushFilter(filter exprInfo) error
	pushKeyFilter(filter exprInfo, table, field string) error
	pushHaving(having exprInfo) error
	pushOrderBy(orderBy sqlparser.OrderBy) error
	pushLimit(limit *sqlparser.Limit) error
	pushMisc(sel *sqlparser.Select)
	reOrder(int)
	setParent(p *JoinNode)
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

// setParenthese is only used in JoinNode and MergeNode.
func setParenthese(node PlanNode, hasParen bool) {
	switch node := node.(type) {
	case *JoinNode:
		node.hasParen = hasParen
	case *MergeNode:
		node.hasParen = hasParen
	}
}

func findParent(tables []string, node PlanNode) PlanNode {
	var parent PlanNode
	for _, tb := range tables {
		tbInfo := node.getReferTables()[tb]
		if parent == nil {
			parent = tbInfo.parent
			continue
		}
		if parent != tbInfo.parent {
			parent = findLCA(node, parent, tbInfo.parent)
		}
	}
	return parent
}

func addFilter(s PlanNode, filter exprInfo) {
	switch node := s.(type) {
	case *JoinNode:
		node.otherFilter = append(node.otherFilter, filter)
	case *MergeNode:
		node.addWhere(filter.expr)
	}
}

// pushFilters push a WHERE clause down, and update the PlanNode info.
func (b *planBuilder) pushFilters(expr sqlparser.Expr) error {
	joins, filters, err := parseWhereOrJoinExprs(expr, b.root.getReferTables())
	if err != nil {
		return err
	}

	for _, filter := range filters {
		if err := b.root.pushFilter(filter); err != nil {
			return err
		}
	}

	switch node := b.root.(type) {
	case *MergeNode:
		for _, joinCond := range joins {
			node.addWhere(joinCond.expr)
		}
	case *JoinNode:
		b.root = node.pushEqualCmprs(joins)
		return nil
	}
	return nil
}

// pushHavings push a HAVING clause down.
func (b *planBuilder) pushHavings(expr sqlparser.Expr) error {
	havings, err := parseHaving(expr, b.root.getFields())
	if err != nil {
		return err
	}
	for _, having := range havings {
		if err = b.root.pushHaving(having); err != nil {
			return err
		}
	}
	return nil
}
