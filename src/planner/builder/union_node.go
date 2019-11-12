/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

import (
	"xcontext"

	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// UnionNode represents union plan.
type UnionNode struct {
	log         *xlog.Log
	Left, Right PlanNode
	// Union Type.
	Typ      string
	children []ChildPlan
	// referred tables' tableInfo map.
	referTables map[string]*tableInfo
}

func newUnionNode(log *xlog.Log, left, right PlanNode, typ string) *UnionNode {
	return &UnionNode{
		log:   log,
		Left:  left,
		Right: right,
		Typ:   typ,
	}
}

// buildQuery used to build the QueryTuple.
func (u *UnionNode) buildQuery(root PlanNode) {
	u.Left.buildQuery(u.Left)
	u.Right.buildQuery(u.Right)
}

// Children returns the children of the plan.
func (u *UnionNode) Children() []ChildPlan {
	return u.children
}

// getReferTables get the referTables.
func (u *UnionNode) getReferTables() map[string]*tableInfo {
	return u.referTables
}

// GetQuery used to get the Querys.
func (u *UnionNode) GetQuery() []xcontext.QueryTuple {
	querys := u.Left.GetQuery()
	querys = append(querys, u.Right.GetQuery()...)
	return querys
}

func (u *UnionNode) getFields() []selectTuple {
	return u.Left.getFields()
}

// pushOrderBy used to push the order by exprs.
func (u *UnionNode) pushOrderBy(sel sqlparser.SelectStatement) error {
	node := sel.(*sqlparser.Union)
	if len(node.OrderBy) > 0 {
		orderPlan := NewOrderByPlan(u.log, node.OrderBy, u.getFields(), u.referTables)
		if err := orderPlan.Build(); err != nil {
			return err
		}
		u.children = append(u.children, orderPlan)
	}
	return nil
}

// pushLimit used to push limit.
func (u *UnionNode) pushLimit(sel sqlparser.SelectStatement) error {
	node := sel.(*sqlparser.Union)
	if node.Limit != nil {
		limitPlan := NewLimitPlan(u.log, node.Limit)
		if err := limitPlan.Build(); err != nil {
			return err
		}
		u.children = append(u.children, limitPlan)
	}
	return nil
}
