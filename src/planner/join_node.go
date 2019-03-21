/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"fmt"
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
	// whether the right node has filters in left join.
	HasRightFilter bool
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
	 * IsLeftJoin is true, 't1.c=t2.c' parser into otherFilter, else into JoinOn.
	 * '1=1' parser into noTableFilter. 't2.b>2' into tableFilter.
	 */
	tableFilter   []filterTuple
	otherFilter   []sqlparser.Expr
	noTableFilter []sqlparser.Expr
	otherJoinOn   *otherJoin
	// record the `otherJoin.left`'s index in left.fields.
	LeftTmpCols []int
	// keyFilters based on LeftKeys、RightKeys and tableFilter.
	// eg: select * from t1 join t2 on t1.a=t2.a where t1.a=1
	// `t1.a` in LeftKeys, `t1.a=1` in tableFilter. in the map,
	// key is 0, value is tableFilter(`t1.a=1`).
	keyFilters map[int][]filterTuple
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
		keyFilters:     make(map[int][]filterTuple),
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
			continue
		}
		if len(filter.referTables) == 1 {
			tb := filter.referTables[0]
			tbInfo := j.referredTables[tb]
			if filter.col == nil {
				tbInfo.parent.setWhereFilter(filter.expr)
			} else {
				// if left join's right node's is null condition will not be pushed down.
				if j.IsLeftJoin && checkIsWithNull(filter.expr) {
					if _, ok := j.Right.getReferredTables()[tb]; ok {
						j.setWhereFilter(filter.expr)
						continue
					}
				}
				j.tableFilter = append(j.tableFilter, filter)
				if tbInfo.parent.index == -1 && filter.val != nil && tbInfo.shardKey != "" {
					if nameMatch(filter.col, tb, tbInfo.shardKey) {
						if tbInfo.parent.index, err = j.router.GetIndex(tbInfo.database, tbInfo.tableName, filter.val); err != nil {
							return err
						}
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
		if j.IsLeftJoin && !j.HasRightFilter {
			if checkFilterInNode(filter, j.Right.getReferredTables()) {
				j.HasRightFilter = true
			}
		}
	}
	return err
}

// setParent set the parent node.
func (j *JoinNode) setParent(p PlanNode) {
	j.parent = p
}

// setWhereFilter set the otherFilter.
func (j *JoinNode) setWhereFilter(filter sqlparser.Expr) {
	j.otherFilter = append(j.otherFilter, filter)
}

// setNoTableFilter used to push the no table filters.
func (j *JoinNode) setNoTableFilter(exprs []sqlparser.Expr) {
	j.noTableFilter = append(j.noTableFilter, exprs...)
}

// otherJoin is the filter in leftjoin's on clause.
// based on the plan tree,separate the otherjoinon.
type otherJoin struct {
	// no tables filter in otherjoinon.
	noTables []sqlparser.Expr
	// filter belong to the left node.
	left []selectTuple
	// fliter belong to the right node.
	// fiiter cross the left and right.
	right, others []filterTuple
}

// setOtherJoin use to process the otherjoinon.
func (j *JoinNode) setOtherJoin(filters []filterTuple) {
	j.otherJoinOn = &otherJoin{}
	i := 0
	for _, filter := range filters {
		if len(filter.referTables) == 0 {
			j.otherJoinOn.noTables = append(j.otherJoinOn.noTables, filter.expr)
			continue
		}
		if checkFilterInNode(filter, j.Left.getReferredTables()) {
			alias := fmt.Sprintf("tmpc_%d", i)
			field := selectTuple{
				expr:        &sqlparser.AliasedExpr{Expr: filter.expr, As: sqlparser.NewColIdent(alias)},
				field:       alias,
				referTables: filter.referTables,
			}
			j.otherJoinOn.left = append(j.otherJoinOn.left, field)
			i++
		} else if checkFilterInNode(filter, j.Right.getReferredTables()) {
			j.otherJoinOn.right = append(j.otherJoinOn.right, filter)
		} else {
			j.otherJoinOn.others = append(j.otherJoinOn.others, filter)
		}
	}
}

// pushOtherJoin use to push otherjoin.
// eg: select A.a from A left join B on A.id=B.id and 1=1 and A.c=1 and B.b='a';
// push: select A.c=1 as tmpc_0,A.a,A.id from A order by A.id asc;
//       select B.id from B where 1=1 and B.b='a' order by B.id asc;
func (j *JoinNode) pushOtherJoin() error {
	if j.otherJoinOn != nil {
		if len(j.otherJoinOn.others) > 0 {
			return errors.New("unsupported: on.clause.in.cross-shard.join")
		}
		if len(j.otherJoinOn.noTables) > 0 {
			j.Right.setNoTableFilter(j.otherJoinOn.noTables)
		}
		if len(j.otherJoinOn.left) > 0 {
			for _, field := range j.otherJoinOn.left {
				index, err := j.Left.pushSelectExpr(field)
				if err != nil {
					return err
				}
				j.LeftTmpCols = append(j.LeftTmpCols, index)
			}
		}
		if len(j.otherJoinOn.right) > 0 {
			for _, filter := range j.otherJoinOn.right {
				var parent PlanNode
				for _, tb := range filter.referTables {
					tbInfo := j.referredTables[tb]
					if parent == nil {
						parent = tbInfo.parent
						continue
					}
					if parent != tbInfo.parent {
						parent = findLCA(j.Right, parent, tbInfo.parent)
					}
				}
				if mn, ok := parent.(*MergeNode); ok {
					mn.setWhereFilter(filter.expr)
				} else {
					return errors.New("unsupported: on.clause.in.cross-shard.join")
				}
			}
		}
	}
	return nil
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

						for _, filter := range node.tableFilter {
							mn.setWhereFilter(filter.expr)
						}
						for _, filter := range node.otherFilter {
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
	for _, filter := range j.tableFilter {
		if !j.buildKeyFilter(filter, false) {
			tbInfo := j.referredTables[filter.referTables[0]]
			tbInfo.parent.setWhereFilter(filter.expr)
		}
	}
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
				mn, _ := mergeRoutes(lmn, rmn, j.joinExpr, nil)
				mn.setParent(j.parent)
				mn.setParenthese(j.hasParen)
				for _, filter := range j.otherFilter {
					mn.setWhereFilter(filter)
				}
				for _, filters := range j.keyFilters {
					for _, filter := range filters {
						mn.setWhereFilter(filter.expr)
					}
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

	if err = j.pushOtherJoin(); err != nil {
		return nil, err
	}
	if len(j.otherFilter) > 0 {
		return nil, errors.New("unsupported: where.clause.in.cross-shard.join")
	}
	return j, nil
}

// buildKeyFilter used to build the keyFilter based on the tableFilter and JoinOn.
// eg: select t1.a,t2.a from t1 join t2 on t1.a=t2.a where t1.a=1;
// push: select t1.a from t1 where t1.a=1 order by t1.a asc;
//       select t2.a from t2 where t2.a=1 order by t2.a asc;
func (j *JoinNode) buildKeyFilter(filter filterTuple, isFind bool) bool {
	table := filter.col.Qualifier.Name.String()
	field := filter.col.Name.String()
	find := false
	if _, ok := j.Left.getReferredTables()[filter.referTables[0]]; ok {
		for i, join := range j.JoinOn {
			lt := join.left.Qualifier.Name.String()
			lc := join.left.Name.String()
			if lt == table && lc == field {
				j.keyFilters[i] = append(j.keyFilters[i], filter)
				if filter.val != nil {
					rt := join.right.Qualifier.Name.String()
					rc := join.right.Name.String()
					tbInfo := j.referredTables[rt]
					if tbInfo.parent.index == -1 && tbInfo.shardKey == rc {
						tbInfo.parent.index, _ = j.router.GetIndex(tbInfo.database, tbInfo.tableName, filter.val)
					}
				}
				find = true
				break
			}
		}
		if jn, ok := j.Left.(*JoinNode); ok {
			return jn.buildKeyFilter(filter, find || isFind)
		}
	} else {
		for i, join := range j.JoinOn {
			rt := join.right.Qualifier.Name.String()
			rc := join.right.Name.String()
			if rt == table && rc == field {
				j.keyFilters[i] = append(j.keyFilters[i], filter)
				if filter.val != nil {
					lt := join.left.Qualifier.Name.String()
					lc := join.left.Name.String()
					tbInfo := j.referredTables[lt]
					if tbInfo.parent.index == -1 && tbInfo.shardKey == lc {
						tbInfo.parent.index, _ = j.router.GetIndex(tbInfo.database, tbInfo.tableName, filter.val)
					}
				}
				find = true
				break
			}
		}
		if jn, ok := j.Right.(*JoinNode); ok {
			return jn.buildKeyFilter(filter, find || isFind)
		}
	}
	return find || isFind
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
		if len(tuple.referTables) == 1 && table == tuple.referTables[0] && field == tuple.field {
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
	j.Left.setNoTableFilter(j.noTableFilter)
	for i, filters := range j.keyFilters {
		table := j.LeftKeys[i].Table
		field := j.LeftKeys[i].Field
		tbInfo := j.referredTables[table]
		for _, filter := range filters {
			filter.col.Qualifier.Name = sqlparser.NewTableIdent(table)
			filter.col.Name = sqlparser.NewColIdent(field)
			tbInfo.parent.filters[filter.expr] = 0
		}
	}
	j.Left.buildQuery()

	j.Right.setNoTableFilter(j.noTableFilter)
	for i, filters := range j.keyFilters {
		table := j.RightKeys[i].Table
		field := j.RightKeys[i].Field
		tbInfo := j.referredTables[table]
		for _, filter := range filters {
			filter.col.Qualifier.Name = sqlparser.NewTableIdent(table)
			filter.col.Name = sqlparser.NewColIdent(field)
			tbInfo.parent.filters[filter.expr] = 0
		}
	}
	j.Right.buildQuery()
}

// GetQuery used to get the Querys.
func (j *JoinNode) GetQuery() []xcontext.QueryTuple {
	querys := j.Left.GetQuery()
	querys = append(querys, j.Right.GetQuery()...)
	return querys
}
