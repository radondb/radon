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
				if nameMatch(filter.col, tbInfo.tableName, tbInfo.shardKey) {
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
