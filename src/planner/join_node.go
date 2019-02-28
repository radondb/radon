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
			if lmn, ok := node.Left.(*MergeNode); ok {
				if rmn, ok := node.Right.(*MergeNode); ok {
					Left := joinFilter.expr.Left.(*sqlparser.ColName)
					Right := joinFilter.expr.Right.(*sqlparser.ColName)
					if isSameShard(lmn.referredTables, rmn.referredTables, Left, Right) {
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
						mn.setWhereFilter(joinFilter.expr)
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
				node.setWhereFilter(joinFilter.expr)
			} else {
				node.joinOn = append(node.joinOn, joinFilter)
				if node.joinExpr != nil {
					node.joinExpr.On = &sqlparser.AndExpr{
						Left:  node.joinExpr.On,
						Right: joinFilter.expr,
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
