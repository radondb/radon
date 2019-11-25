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

// UnionNode ...
// eg: select a, b from t1 union select t2.a,t3.b from t2 join t3 on t2.id=t3.id;
//             PlanNode1
//               /  \
//              /    \
//        PlanNode2  PlanNode3
// PlanNode1: UnionNode
// PlanNode2: MergeNode
// PlanNode3: JoinNode
//             /  \
//            /    \
//      MergeNode  MergeNode
// PlanNode2 and PlanNode3 are two independent trees, and they are also the Left
// and Right of PlanNode1.
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
func (u *UnionNode) pushOrderBy(orderBy sqlparser.OrderBy) error {
	orderPlan := NewOrderByPlan(u.log, orderBy, u)
	u.children = append(u.children, orderPlan)
	return orderPlan.Build()
}

// pushLimit used to push limit.
func (u *UnionNode) pushLimit(limit *sqlparser.Limit) error {
	limitPlan := NewLimitPlan(u.log, limit)
	u.children = append(u.children, limitPlan)
	return limitPlan.Build()
}

// Temporarily unreachable.
func (u *UnionNode) calcRoute() (PlanNode, error) {
	panic("unreachable")
}

// Temporarily unreachable.
func (u *UnionNode) pushFilter(filter exprInfo) error {
	panic("unreachable")
}

// Temporarily unreachable.
func (u *UnionNode) pushKeyFilter(filter exprInfo, table, field string) error {
	panic("unreachable")
}

// Temporarily unreachable.
func (u *UnionNode) pushSelectExpr(field selectTuple) (int, error) {
	panic("unreachable")
}

// Temporarily unreachable.
func (u *UnionNode) pushHaving(having exprInfo) error {
	panic("unreachable")
}

// Temporarily unreachable.
func (u *UnionNode) pushMisc(sel *sqlparser.Select) {
	panic("unreachable")
}

// Temporarily unreachable.
func (u *UnionNode) addNoTableFilter(exprs []sqlparser.Expr) {
	panic("unreachable")
}

// unreachable.
func (u *UnionNode) pushSelectExprs(fields, groups []selectTuple, sel *sqlparser.Select, aggTyp aggrType) error {
	panic("unreachable")
}

// unreachable.
func (u *UnionNode) setParent(p *JoinNode) {
	panic("unreachable")
}

// unreachable.
func (u *UnionNode) reOrder(int) {
	panic("unreachable")
}

// Order unreachable.
func (u *UnionNode) Order() int {
	panic("unreachable")
}
