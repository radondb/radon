/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"router"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// JoinNode cannot be pushed down.
type JoinNode struct {
	log *xlog.Log
	// router.
	router *router.Router
	// Left and Right are the nodes for the join.
	Left, Right PlanNode
	// JoinTableExpr in FROM clause.
	joinExpr *sqlparser.JoinTableExpr
	// referred tables' tableInfo map.
	referredTables map[string]*TableInfo
	// whether has parenthese in FROM clause.
	hasParen bool
	// whether is left join.
	isLeftJoin bool
	// parent node in the plan tree.
	parent PlanNode
	// children plans in select(such as: orderby, limit..).
	children *PlanTree
	// join on condition tuples.
	joinOn []joinTuple
	/*
	 * eg: 't1 left join t2 on t1.a=t2.a and t1.b=2' where t1.c=t2.c and 1=1 and t2.b>2.
	 * 't1.b=2' will parser into otherJoinOn, only leftjoin exists otherJoinOn.
	 * isLeftJoin is true, 't1.c=t2.c' parser into whereFilter, else into joinOn.
	 * '1=1' parser into noTableFilter.
	 */
	otherJoinOn   []filterTuple
	whereFilter   []sqlparser.Expr
	noTableFilter []sqlparser.Expr
}

// newJoinNode used to create JoinNode.
func newJoinNode(log *xlog.Log, Left, Right PlanNode, router *router.Router, joinExpr *sqlparser.JoinTableExpr,
	joinOn []joinTuple, referredTables map[string]*TableInfo) *JoinNode {
	isLeftJoin := false
	if joinExpr != nil && joinExpr.Join == sqlparser.LeftJoinStr {
		isLeftJoin = true
	}
	return &JoinNode{
		log:            log,
		Left:           Left,
		Right:          Right,
		router:         router,
		joinExpr:       joinExpr,
		joinOn:         joinOn,
		otherJoinOn:    make([]filterTuple, 0, 4),
		whereFilter:    make([]sqlparser.Expr, 0, 4),
		noTableFilter:  make([]sqlparser.Expr, 0, 4),
		referredTables: referredTables,
		isLeftJoin:     isLeftJoin,
		children:       NewPlanTree(),
	}
}

// getReferredTables get the referredTables.
func (j *JoinNode) getReferredTables() map[string]*TableInfo {
	return j.referredTables
}

// setParenthese set hasParen.
func (j *JoinNode) setParenthese(hasParen bool) {
	j.hasParen = hasParen
}

// pushFilter used to push the filters.
func (j *JoinNode) pushFilter(filters []filterTuple) error {
	var err error
	for _, filter := range filters {
		if len(filter.referTables) == 0 {
			j.noTableFilter = append(j.noTableFilter, filter.expr)
		} else if len(filter.referTables) == 1 {
			tbInfo, _ := j.referredTables[filter.referTables[0]]
			tbInfo.whereFilter = append(tbInfo.whereFilter, filter.expr)
			if tbInfo.parent.index == -1 && filter.col != nil && tbInfo.shardKey != "" {
				if nameMatch(filter.col, filter.referTables[0], tbInfo.shardKey) {
					if sqlval, ok := filter.val.(*sqlparser.SQLVal); ok {
						if tbInfo.parent.index, err = j.router.GetIndex(tbInfo.database, tbInfo.tableName, sqlval); err != nil {
							return err
						}
					}
				}
			}
		} else {
			var parent PlanNode
			for _, tb := range filter.referTables {
				tbInfo, _ := j.referredTables[tb]
				if parent == nil {
					parent = tbInfo.parent
					continue
				}
				if parent != tbInfo.parent {
					parent = findLCA(j, parent, tbInfo.parent)
				}
			}
			parent.setWhereFilter(filter.expr)
		}
	}
	return err
}

// setParent set the parent node.
func (j *JoinNode) setParent(p PlanNode) {
	j.parent = p
}

// setWhereFilter set the whereFilter.
func (j *JoinNode) setWhereFilter(filter sqlparser.Expr) {
	j.whereFilter = append(j.whereFilter, filter)
}

// setNoTableFilter used to push the no table filters.
func (j *JoinNode) setNoTableFilter(exprs []sqlparser.Expr) {
	j.noTableFilter = exprs
}

// pushJoinInWhere used to push the 'join' type filters.
// eg: 'select * from t1, t2 where t1.a=t2.a and t1.b=2'.
// 't1.a=t2.a' is the 'join' type filters.
func (j *JoinNode) pushJoinInWhere(joins []joinTuple) (PlanNode, error) {
	for i, joinFilter := range joins {
		var parent PlanNode
		ltb, _ := j.referredTables[joinFilter.referTables[0]]
		rtb, _ := j.referredTables[joinFilter.referTables[1]]
		parent = findLCA(j, ltb.parent, rtb.parent)

		switch node := parent.(type) {
		case *MergeNode:
			node.setWhereFilter(joinFilter.expr)
		case *JoinNode:
			join, _ := checkJoinOn(node.Left, node.Right, joinFilter)
			if lmn, ok := node.Left.(*MergeNode); ok {
				if rmn, ok := node.Right.(*MergeNode); ok {
					if isSameShard(lmn.referredTables, rmn.referredTables, join.left, join.right) {
						mn, _ := mergeRoutes(lmn, rmn, node.joinExpr, nil)
						mn.setParent(node.parent)
						mn.setParenthese(node.hasParen)

						for _, filter := range node.whereFilter {
							mn.setWhereFilter(filter)
						}
						for _, exprs := range node.noTableFilter {
							mn.setWhereFilter(exprs)
						}

						if node.joinExpr == nil {
							for _, joins := range node.joinOn {
								mn.setWhereFilter(joins.expr)
							}
						}
						mn.setWhereFilter(join.expr)
						if node.parent == nil {
							return mn.pushJoinInWhere(joins[i+1:])
						}

						j := node.parent.(*JoinNode)
						if j.Left == node {
							j.Left = mn
						} else {
							j.Right = mn
						}
						continue
					}
				}
			}
			if node.isLeftJoin {
				node.setWhereFilter(join.expr)
			} else {
				node.joinOn = append(node.joinOn, join)
				if node.joinExpr != nil {
					node.joinExpr.On = &sqlparser.AndExpr{
						Left:  node.joinExpr.On,
						Right: join.expr,
					}
				}
			}
		}
	}
	return j, nil
}

// calcRoute used to calc the route.
func (j *JoinNode) calcRoute() (PlanNode, error) {
	var err error
	if j.Left, err = j.Left.calcRoute(); err != nil {
		return j, err
	}
	if j.Right, err = j.Right.calcRoute(); err != nil {
		return j, err
	}

	// left and right node have same routes.
	if lmn, ok := j.Left.(*MergeNode); ok {
		if rmn, ok := j.Right.(*MergeNode); ok {
			if (lmn.backend != "" && lmn.backend == rmn.backend) || rmn.shardCount == 0 || lmn.shardCount == 0 {
				if lmn.shardCount == 0 {
					lmn.backend = rmn.backend
					lmn.routeLen = rmn.routeLen
					lmn.index = rmn.index
				}
				mn, err := mergeRoutes(lmn, rmn, j.joinExpr, nil)
				if err != nil {
					return nil, err
				}
				mn.setParent(j.parent)
				mn.setParenthese(j.hasParen)
				for _, filter := range j.whereFilter {
					mn.setWhereFilter(filter)
				}
				for _, exprs := range j.noTableFilter {
					mn.setWhereFilter(exprs)
				}

				if j.joinExpr == nil && len(j.joinOn) > 0 {
					for _, joins := range j.joinOn {
						mn.setWhereFilter(joins.expr)
					}
				}
				return mn, nil
			}
		}
	}
	return j, nil
}

// spliceWhere used to splice where clause.
func (j *JoinNode) spliceWhere() error {
	if len(j.otherJoinOn) > 0 || len(j.whereFilter) > 0 {
		return errors.New("unsupported: where.clause.in.cross-shard.join")
	}
	j.Left.setNoTableFilter(j.noTableFilter)
	if err := j.Left.spliceWhere(); err != nil {
		return err
	}
	j.Right.setNoTableFilter(j.noTableFilter)
	if err := j.Right.spliceWhere(); err != nil {
		return err
	}
	return nil
}

// pushSelectExprs used to push the select fileds.
// TODO: need record original selectexprs order.
func (j *JoinNode) pushSelectExprs(fileds, groups []selectTuple, sel *sqlparser.Select, hasAggregates bool) error {
	if hasAggregates {
		return errors.New("unsupported: cross-shard.query.with.aggregates")
	}
	if len(groups) > 0 {
		aggrPlan := NewAggregatePlan(j.log, sel, fileds, groups)
		if err := aggrPlan.Build(); err != nil {
			return err
		}
		j.children.Add(aggrPlan)
	}
	for _, tuple := range fileds {
		if len(tuple.referTables) == 0 {
			_, tbInfo := getOneTableInfo(j.referredTables)
			tbInfo.parent.sel.SelectExprs = append(tbInfo.parent.sel.SelectExprs, tuple.expr)
		} else if len(tuple.referTables) == 1 {
			tbInfo, _ := j.referredTables[tuple.referTables[0]]
			tbInfo.parent.sel.SelectExprs = append(tbInfo.parent.sel.SelectExprs, tuple.expr)
		} else {
			var parent PlanNode
			for _, tb := range tuple.referTables {
				tbInfo, _ := j.referredTables[tb]
				if parent == nil {
					parent = tbInfo.parent
					continue
				}
				if parent != tbInfo.parent {
					parent = findLCA(j, parent, tbInfo.parent)
				}
			}
			if mn, ok := parent.(*MergeNode); ok {
				mn.sel.SelectExprs = append(mn.sel.SelectExprs, tuple.expr)
			} else {
				return errors.New("unsupported: select.expr.in.cross-shard.join")
			}
		}
	}
	return nil
}

// pushHaving used to push having exprs.
func (j *JoinNode) pushHaving(havings []filterTuple) error {
	for _, filter := range havings {
		if len(filter.referTables) == 0 {
			j.Left.pushHaving([]filterTuple{filter})
			j.Right.pushHaving([]filterTuple{filter})
		} else if len(filter.referTables) == 1 {
			tbInfo, _ := j.referredTables[filter.referTables[0]]
			tbInfo.parent.sel.AddHaving(filter.expr)
		} else {
			var parent PlanNode
			for _, tb := range filter.referTables {
				tbInfo, _ := j.referredTables[tb]
				if parent == nil {
					parent = tbInfo.parent
					continue
				}
				if parent != tbInfo.parent {
					parent = findLCA(j, parent, tbInfo.parent)
				}
			}
			if mn, ok := parent.(*MergeNode); ok {
				mn.sel.AddHaving(filter.expr)
			} else {
				return errors.New("unsupported: havings.in.cross-shard.join")
			}
		}
	}
	return nil
}

// pushOrderBy used to push the order by exprs.
func (j *JoinNode) pushOrderBy(sel *sqlparser.Select, fileds []selectTuple) error {
	if len(sel.OrderBy) == 0 {
		for _, by := range sel.GroupBy {
			sel.OrderBy = append(sel.OrderBy, &sqlparser.Order{
				Expr:      by,
				Direction: sqlparser.AscScr,
			})
		}
	}

	if len(sel.OrderBy) > 0 {
		orderPlan := NewOrderByPlan(j.log, sel, fileds)
		if err := orderPlan.Build(); err != nil {
			return err
		}
		j.children.Add(orderPlan)
	}

	return nil
}

// pushLimit used to push limit.
func (j *JoinNode) pushLimit(sel *sqlparser.Select) error {
	limitPlan := NewLimitPlan(j.log, sel)
	if err := limitPlan.Build(); err != nil {
		return err
	}
	j.children.Add(limitPlan)
	return nil
}

// pushMisc used tp push miscelleaneous constructs.
func (j *JoinNode) pushMisc(sel *sqlparser.Select) {
	j.Left.pushMisc(sel)
	j.Right.pushMisc(sel)
}

// Children returns the children of the plan.
func (j *JoinNode) Children() *PlanTree {
	return j.children
}
