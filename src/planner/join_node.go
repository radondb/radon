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
	"xcontext"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// JoinKey is the column info in the on conditions.
type JoinKey struct {
	// field name.
	Field string
	// table name.
	Table string
	// index in the fields.
	Index int
}

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
	IsLeftJoin bool
	// parent node in the plan tree.
	parent PlanNode
	// children plans in select(such as: orderby, limit..).
	children *PlanTree
	// Cols defines which columns from left or right results used to build the return result.
	// For results coming from left, the values go as -1, -2, etc. For right, they're 1, 2, etc.
	// If Cols is {-1, -2, 1, 2}, it means the returned result is {Left0, Left1, Right0, Right1}.
	Cols []int `json:",omitempty"`
	// the returned result fields.
	fields []selectTuple
	// join on condition tuples.
	JoinOn []joinTuple
	// eg: from t1 join t2 on t1.a=t2.b, 't1.a' put in LeftKeys, 't2.a' in RightKeys.
	LeftKeys, RightKeys []JoinKey
	// if Left is MergeNode and LeftKeys contain unique keys, LeftUnique will be true.
	// used in sort merge join.
	LeftUnique, RightUnique bool
	/*
	 * eg: 't1 left join t2 on t1.a=t2.a and t1.b=2' where t1.c=t2.c and 1=1 and t2.b>2.
	 * 't1.b=2' will parser into otherJoinOn, only leftjoin exists otherJoinOn.
	 * IsLeftJoin is true, 't1.c=t2.c' parser into whereFilter, else into joinOn.
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
		JoinOn:         joinOn,
		otherJoinOn:    make([]filterTuple, 0, 4),
		whereFilter:    make([]sqlparser.Expr, 0, 4),
		noTableFilter:  make([]sqlparser.Expr, 0, 4),
		referredTables: referredTables,
		IsLeftJoin:     isLeftJoin,
		children:       NewPlanTree(),
	}
}

// getReferredTables get the referredTables.
func (j *JoinNode) getReferredTables() map[string]*TableInfo {
	return j.referredTables
}

// getFields get the fields.
func (j *JoinNode) getFields() []selectTuple {
	return j.fields
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
			tbInfo := j.referredTables[filter.referTables[0]]
			tbInfo.whereFilter = append(tbInfo.whereFilter, filter.expr)
			if tbInfo.parent.index == -1 && filter.val != nil && tbInfo.shardKey != "" {
				if nameMatch(filter.col, filter.referTables[0], tbInfo.shardKey) {
					if tbInfo.parent.index, err = j.router.GetIndex(tbInfo.database, tbInfo.tableName, filter.val); err != nil {
						return err
					}
				}
			}
		} else {
			var parent PlanNode
			for _, tb := range filter.referTables {
				tbInfo := j.referredTables[tb]
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
func (j *JoinNode) pushJoinInWhere(joins []joinTuple) PlanNode {
	for i, joinFilter := range joins {
		var parent PlanNode
		ltb := j.referredTables[joinFilter.referTables[0]]
		rtb := j.referredTables[joinFilter.referTables[1]]
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
							for _, joins := range node.JoinOn {
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
			if node.IsLeftJoin {
				node.setWhereFilter(join.expr)
			} else {
				node.JoinOn = append(node.JoinOn, join)
				if node.joinExpr != nil {
					node.joinExpr.On = &sqlparser.AndExpr{
						Left:  node.joinExpr.On,
						Right: join.expr,
					}
				}
			}
		}
	}
	return j
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

				if j.joinExpr == nil && len(j.JoinOn) > 0 {
					for _, joins := range j.JoinOn {
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

// pushSelectExprs used to push the select fields.
func (j *JoinNode) pushSelectExprs(fields, groups []selectTuple, sel *sqlparser.Select, hasAggregates bool) error {
	if hasAggregates {
		return errors.New("unsupported: cross-shard.query.with.aggregates")
	}
	if len(groups) > 0 {
		aggrPlan := NewAggregatePlan(j.log, sel.SelectExprs, fields, groups)
		if err := aggrPlan.Build(); err != nil {
			return err
		}
		j.children.Add(aggrPlan)
	}
	for _, tuple := range fields {
		if _, err := j.pushSelectExpr(tuple); err != nil {
			return err
		}
	}
	j.processJoinOn()

	return nil
}

// pushSelectExpr used to push the select field.
func (j *JoinNode) pushSelectExpr(field selectTuple) (int, error) {
	if checkSelectExpr(field, j.Left.getReferredTables()) {
		index, err := j.Left.pushSelectExpr(field)
		if err != nil {
			return -1, err
		}
		j.Cols = append(j.Cols, -index-1)
	} else if checkSelectExpr(field, j.Right.getReferredTables()) {
		index, err := j.Right.pushSelectExpr(field)
		if err != nil {
			return -1, err
		}
		j.Cols = append(j.Cols, index+1)
	} else {
		return -1, errors.New("unsupported: select.expr.in.cross-shard.join")
	}
	j.fields = append(j.fields, field)
	return len(j.fields) - 1, nil
}

// processJoinOn used to build order by based on On conditions.
func (j *JoinNode) processJoinOn() {
	// eg: select t1.a,t2.a from t1 join t2 on t1.a=t2.a;
	// push: select t1.a from t1 order by t1.a asc;
	//       select t2.a from t2 order by t2.a asc;
	_, lok := j.Left.(*MergeNode)
	_, rok := j.Right.(*MergeNode)
	for _, join := range j.JoinOn {
		leftKey := j.buildOrderBy(join.left, j.Left)
		if lok && !j.LeftUnique {
			j.LeftUnique = (leftKey.Field == j.referredTables[leftKey.Table].shardKey)
		}
		j.LeftKeys = append(j.LeftKeys, leftKey)
		rightKey := j.buildOrderBy(join.right, j.Right)
		if rok && !j.RightUnique {
			j.RightUnique = (rightKey.Field == j.referredTables[rightKey.Table].shardKey)
		}
		j.RightKeys = append(j.RightKeys, rightKey)
	}
}

func (j *JoinNode) buildOrderBy(col *sqlparser.ColName, node PlanNode) JoinKey {
	field := col.Name.String()
	table := col.Qualifier.Name.String()
	tuples := node.getFields()
	index := -1
	for i, tuple := range tuples {
		if table == tuple.referTables[0] && field == tuple.field {
			index = i
			break
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

	if m, ok := node.(*MergeNode); ok {
		m.sel.OrderBy = append(m.sel.OrderBy, &sqlparser.Order{
			Expr:      col,
			Direction: sqlparser.AscScr,
		})
	} else {
		node.(*JoinNode).processJoinOn()
	}

	return JoinKey{field, table, index}
}

// pushHaving used to push having exprs.
func (j *JoinNode) pushHaving(havings []filterTuple) error {
	for _, filter := range havings {
		if len(filter.referTables) == 0 {
			j.Left.pushHaving([]filterTuple{filter})
			j.Right.pushHaving([]filterTuple{filter})
		} else if len(filter.referTables) == 1 {
			tbInfo := j.referredTables[filter.referTables[0]]
			tbInfo.parent.sel.AddHaving(filter.expr)
		} else {
			var parent PlanNode
			for _, tb := range filter.referTables {
				tbInfo := j.referredTables[tb]
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
func (j *JoinNode) pushOrderBy(sel *sqlparser.Select, fields []selectTuple) error {
	if len(sel.OrderBy) == 0 {
		for _, by := range sel.GroupBy {
			sel.OrderBy = append(sel.OrderBy, &sqlparser.Order{
				Expr:      by,
				Direction: sqlparser.AscScr,
			})
		}
	}

	if len(sel.OrderBy) > 0 {
		orderPlan := NewOrderByPlan(j.log, sel, fields, j.referredTables)
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

// buildQuery used to build the QueryTuple.
func (j *JoinNode) buildQuery() {
	j.Left.buildQuery()
	j.Right.buildQuery()
}

// GetQuery used to get the Querys.
func (j *JoinNode) GetQuery() []xcontext.QueryTuple {
	querys := j.Left.GetQuery()
	querys = append(querys, j.Right.GetQuery()...)
	return querys
}
