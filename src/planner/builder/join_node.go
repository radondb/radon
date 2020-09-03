/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

import (
	"strings"

	"router"
	"xcontext"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// JoinStrategy is Join Strategy.
type JoinStrategy int

const (
	// Cartesian product.
	Cartesian JoinStrategy = iota
	// SortMerge Join.
	SortMerge
	// NestLoop Join.
	NestLoop
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

// Comparison is record the sqlparser.Comparison info.
type Comparison struct {
	// index in left and right node's fields.
	Left, Right int
	Operator    string
	// left expr may in right node.
	Exchange bool
}

// otherJoin record join conditions except the equal conditions in left join.
type otherJoin struct {
	// filters without tables.
	noTables []sqlparser.Expr
	// filters belong to JoinNode.Left.
	left []selectTuple
	// filters belong to the JoinNode.Right.
	right []exprInfo
	// filters cross-shard.
	others []exprInfo
}

// JoinNode cannot be pushed down.
type JoinNode struct {
	log *xlog.Log
	// router.
	router *router.Router
	// Left and Right are the nodes for the join.
	Left, Right PlanNode
	// join strategy.
	Strategy JoinStrategy
	// JoinTableExpr in FROM clause.
	joinExpr *sqlparser.JoinTableExpr
	// referred tables' tableInfo map.
	referTables map[string]*tableInfo
	// whether has parenthese in FROM clause.
	hasParen bool
	// parent node in the plan tree.
	parent *JoinNode
	// children plans in select(such as: orderby, limit..).
	children []ChildPlan
	// Cols defines which columns from left or right results used to build the return result.
	// For results coming from left, the values go as -1, -2, etc. For right, they're 1, 2, etc.
	// If Cols is {-1, -2, 1, 2}, it means the returned result is {Left0, Left1, Right0, Right1}.
	Cols []int `json:",omitempty"`
	// the returned result fields.
	fields []selectTuple
	// the equal join conditions.
	// eg: `t1.a=t2.a` in `t1 join t2 on t1.a=t2.a`.
	joinOn []exprInfo
	// eg: from t1 join t2 on t1.a=t2.b, 't1.a' put in LeftKeys, 't2.a' in RightKeys.
	LeftKeys, RightKeys []JoinKey
	// eg: t1 join t2 on t1.a>t2.a, 't1.a>t2.a' parse into CmpFilter.
	CmpFilter []Comparison
	/*
	 * eg: 't1 left join t2 on t1.a=t2.a and t1.b=2' where t1.c=t2.c and 1=1 and t2.b>2 and t2.str is null.
	 * 't1.b=2' will parse into otherLeftJoin. If join type is leftjoin, 't1.c=t2.c' parse into otherFilter,
	 * otherwise parse into joinOn. '1=1' parse into noTableFilter. 't2.str is null' parse into rightNull.
	 */
	// The cross-shard filters.
	otherFilter []exprInfo
	// The filters without tables, always true or false.
	noTableFilter []sqlparser.Expr
	// The join conditions except for the equal condition in left join.
	otherLeftJoin *otherJoin
	// The null conditions belong to JoinNode.Right, when join type is left join.
	rightNull []selectTuple
	// whether is left join.
	IsLeftJoin bool
	// whether the right node has filters in left join.
	HasRightFilter bool
	// record the `otherLeftJoin.left`'s index in left.fields.
	LeftTmpCols []int
	// record the `rightNull`'s index in right.fields.
	RightTmpCols []int
	order        int
	// Vars defines the list of joinVars that need to be built
	// from the Left result before invoking the Right subqquery.
	Vars map[string]int
}

// newJoinNode used to create JoinNode.
func newJoinNode(log *xlog.Log, Left, Right PlanNode, router *router.Router, joinExpr *sqlparser.JoinTableExpr,
	joinOn []exprInfo, referTables map[string]*tableInfo) *JoinNode {
	isLeftJoin := false
	if joinExpr != nil && joinExpr.Join == sqlparser.LeftJoinStr {
		isLeftJoin = true
	}
	return &JoinNode{
		log:         log,
		Left:        Left,
		Right:       Right,
		router:      router,
		joinExpr:    joinExpr,
		joinOn:      joinOn,
		Vars:        make(map[string]int),
		referTables: referTables,
		IsLeftJoin:  isLeftJoin,
		Strategy:    SortMerge,
	}
}

// getReferTables get the referTables.
func (j *JoinNode) getReferTables() map[string]*tableInfo {
	return j.referTables
}

// getFields get the fields.
func (j *JoinNode) getFields() []selectTuple {
	return j.fields
}

// pushFilter used to push the filter.
func (j *JoinNode) pushFilter(filter exprInfo) error {
	if len(filter.referTables) == 0 {
		j.noTableFilter = append(j.noTableFilter, filter.expr)
		return nil
	}

	rightTbs := j.Right.getReferTables()
	if j.IsLeftJoin {
		// If left join's right node has "is null" condition, the condition
		// will record in rightNull instead of push down.
		if ok, nullFunc := checkIsWithNull(filter, rightTbs); ok {
			j.rightNull = append(j.rightNull, nullFunc)
			return nil
		}

		if !j.HasRightFilter {
			for _, tb := range filter.referTables {
				if _, ok := rightTbs[tb]; ok {
					j.HasRightFilter = true
					break
				}
			}
		}
	}

	if len(filter.referTables) == 1 {
		tb := filter.referTables[0]
		tbInfo := j.referTables[tb]
		if len(filter.cols) != 1 {
			addFilter(tbInfo.parent, filter)
		} else {
			if err := j.pushKeyFilter(filter, filter.cols[0].Qualifier.Name.String(), filter.cols[0].Name.String()); err != nil {
				return err
			}
		}
	} else {
		parent := findParent(filter.referTables, j)
		addFilter(parent, filter)
	}
	return nil
}

// pushKeyFilter used to build the keyFilter based on the filter and joinOn.
// eg: select t1.a,t2.a from t1 join t2 on t1.a=t2.a where t1.a=1;
// push: select t1.a from t1 where t1.a=1 order by t1.a asc;
//       select t2.a from t2 where t2.a=1 order by t2.a asc;
func (j *JoinNode) pushKeyFilter(filter exprInfo, table, field string) error {
	var tb, col string
	var err error
	find := false
	if _, ok := j.Left.getReferTables()[table]; ok {
		for _, join := range j.joinOn {
			lt := join.cols[0].Qualifier.Name.String()
			lc := join.cols[0].Name.String()
			if lt == table && lc == field {
				tb = join.cols[1].Qualifier.Name.String()
				col = join.cols[1].Name.String()
				find = true
				break
			}
		}

		if err = j.Left.pushKeyFilter(filter, table, field); err != nil {
			return err
		}

		if find {
			// replace the colname.
			origin := *(filter.cols[0])
			filter.cols[0].Name = sqlparser.NewColIdent(col)
			filter.cols[0].Qualifier = sqlparser.TableName{Name: sqlparser.NewTableIdent(tb)}
			if err = j.Right.pushKeyFilter(filter, tb, col); err != nil {
				return err
			}
			// recover the colname in exprisson.
			*(filter.cols[0]) = origin
		}
	} else {
		for _, join := range j.joinOn {
			rt := join.cols[1].Qualifier.Name.String()
			rc := join.cols[1].Name.String()
			if rt == table && rc == field {
				tb = join.cols[0].Qualifier.Name.String()
				col = join.cols[0].Name.String()
				find = true
				break
			}
		}
		if err = j.Right.pushKeyFilter(filter, table, field); err != nil {
			return err
		}
		if find {
			origin := *(filter.cols[0])
			filter.cols[0].Name = sqlparser.NewColIdent(col)
			filter.cols[0].Qualifier = sqlparser.TableName{Name: sqlparser.NewTableIdent(tb)}
			if err = j.Left.pushKeyFilter(filter, tb, col); err != nil {
				return err
			}
			*(filter.cols[0]) = origin
		}
	}
	return nil
}

// setParent set the parent node.
func (j *JoinNode) setParent(p *JoinNode) {
	j.parent = p
}

// addNoTableFilter used to push the no table filters.
func (j *JoinNode) addNoTableFilter(exprs []sqlparser.Expr) {
	j.noTableFilter = append(j.noTableFilter, exprs...)
}

// setOtherJoin separate filters into j.otherLeftJoin.
func (j *JoinNode) setOtherJoin(filters []exprInfo) {
	j.otherLeftJoin = &otherJoin{}
	for _, filter := range filters {
		if len(filter.referTables) == 0 {
			j.otherLeftJoin.noTables = append(j.otherLeftJoin.noTables, filter.expr)
			continue
		}
		if checkTbInNode(filter.referTables, j.Left.getReferTables()) {
			buf := sqlparser.NewTrackedBuffer(nil)
			filter.expr.Format(buf)
			field := buf.String()

			alias := "tmpc"
			tuple := selectTuple{
				expr:  &sqlparser.AliasedExpr{Expr: filter.expr, As: sqlparser.NewColIdent(alias)},
				info:  filter,
				field: field,
				alias: alias,
			}
			j.otherLeftJoin.left = append(j.otherLeftJoin.left, tuple)
		} else if checkTbInNode(filter.referTables, j.Right.getReferTables()) {
			j.otherLeftJoin.right = append(j.otherLeftJoin.right, filter)
		} else {
			j.otherLeftJoin.others = append(j.otherLeftJoin.others, filter)
		}
	}
}

// pushOthers use to push otherLeftJoin and otherFilter.
// eg: select A.a from A left join B on A.id=B.id and 1=1 and A.c=1 and B.b='a';
// push: select A.c=1 as tmpc_0,A.a,A.id from A order by A.id asc;
//       select B.id from B where 1=1 and B.b='a' order by B.id asc;
func (j *JoinNode) pushOthers() error {
	if j.otherLeftJoin != nil {
		if len(j.otherLeftJoin.noTables) > 0 {
			j.Right.addNoTableFilter(j.otherLeftJoin.noTables)
		}

		for _, field := range j.otherLeftJoin.left {
			index, err := j.Left.pushSelectExpr(field)
			if err != nil {
				return err
			}
			j.LeftTmpCols = append(j.LeftTmpCols, index)
		}

		// If its parent is JoinNode, add it to parent.otherFilter.
		for _, filter := range j.otherLeftJoin.right {
			parent := findParent(filter.referTables, j.Right)
			addFilter(parent, filter)
		}

		if len(j.otherLeftJoin.others) > 0 {
			j.judgeStrategy(j.otherLeftJoin.others)
			if err := j.pushOtherFilters(j.otherLeftJoin.others, true); err != nil {
				return err
			}
		}
	}

	j.judgeStrategy(j.otherFilter)
	return j.pushOtherFilters(j.otherFilter, false)
}

// judgeStrategy judge the join strategy by analyze `otherLeftJoin.others` and `otherFilter`.
// SortMerge request the cross-shard expression must be a `ComparisonExpr` and its `left`
// and `right`'s parent must be `MergeNode`. Otherwise set the join strategy to NestLoop.
func (j *JoinNode) judgeStrategy(filters []exprInfo) {
	for _, filter := range filters {
		if j.Strategy == NestLoop {
			return
		}

		if exp, ok := filter.expr.(*sqlparser.ComparisonExpr); ok {
			left := findParent(getTbsInExpr(exp.Left), j)
			if _, ok := left.(*JoinNode); ok {
				j.setNestLoop()
				return
			}
			right := findParent(getTbsInExpr(exp.Right), j)
			if _, ok := right.(*JoinNode); ok {
				j.setNestLoop()
			}
		} else {
			j.setNestLoop()
		}
	}
}

// setNestLoop set the strategy to Nest Loop.
func (j *JoinNode) setNestLoop() {
	if left, ok := j.Left.(*JoinNode); ok {
		left.setNestLoop()
	}
	if right, ok := j.Right.(*JoinNode); ok {
		right.setNestLoop()
	}
	j.Strategy = NestLoop
}

// pushEqualCmpr used to push the equal Comparison type filters.
// eg: 'select * from t1, t2 where t1.a=t2.a and t1.b=2'.
// 't1.a=t2.a' is the 'join' type filters.
func (j *JoinNode) pushEqualCmprs(joins []exprInfo) PlanNode {
	for i, joinCond := range joins {
		var parent PlanNode
		ltb := j.referTables[joinCond.referTables[0]]
		rtb := j.referTables[joinCond.referTables[1]]
		parent = findLCA(j, ltb.parent, rtb.parent)

		switch node := parent.(type) {
		case *MergeNode:
			node.addWhere(joinCond.expr)
		case *JoinNode:
			join, _ := checkJoinOn(node.Left, node.Right, joinCond)
			if lmn, ok := node.Left.(*MergeNode); ok {
				if rmn, ok := node.Right.(*MergeNode); ok {
					if isSameShard(lmn.referTables, rmn.referTables, join.cols[0], join.cols[1]) {
						mn, _ := mergeRoutes(lmn, rmn, node.joinExpr, nil)
						mn.setParent(node.parent)
						setParenthese(mn, node.hasParen)

						for _, filter := range node.otherFilter {
							mn.addWhere(filter.expr)
						}
						for _, expr := range node.noTableFilter {
							mn.addWhere(expr)
						}

						if node.joinExpr == nil {
							for _, joins := range node.joinOn {
								mn.addWhere(joins.expr)
							}
						}
						mn.addWhere(join.expr)
						if node.parent == nil {
							for _, joinCond := range joins[i+1:] {
								mn.addWhere(joinCond.expr)
							}
							return mn
						}

						if node.parent.Left == node {
							node.parent.Left = mn
						} else {
							node.parent.Right = mn
						}
						continue
					}
				}
			}
			if node.IsLeftJoin {
				node.otherFilter = append(node.otherFilter, join)
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
			if (lmn.backend != "" && lmn.backend == rmn.backend) || rmn.nonGlobalCnt == 0 || lmn.nonGlobalCnt == 0 {
				if lmn.nonGlobalCnt == 0 {
					lmn.backend = rmn.backend
					lmn.routeLen = rmn.routeLen
					lmn.indexes = rmn.indexes
				}
				mn, _ := mergeRoutes(lmn, rmn, j.joinExpr, nil)
				mn.setParent(j.parent)
				setParenthese(mn, j.hasParen)
				for _, filter := range j.otherFilter {
					mn.addWhere(filter.expr)
				}
				mn.addNoTableFilter(j.noTableFilter)
				if j.joinExpr == nil && len(j.joinOn) > 0 {
					for _, joinCond := range j.joinOn {
						mn.addWhere(joinCond.expr)
					}
				}
				return mn, nil
			}
		}
	}
	return j, nil
}

// pushSelectExprs used to push the select fields.
func (j *JoinNode) pushSelectExprs(fields, groups []selectTuple, sel *sqlparser.Select, aggTyp aggrType) error {
	j.reOrder(0)

	if len(groups) > 0 || aggTyp != nullAgg {
		aggrPlan := NewAggregatePlan(j.log, sel.SelectExprs, fields, groups, false)
		if err := aggrPlan.Build(); err != nil {
			return err
		}
		j.children = append(j.children, aggrPlan)
		fields = aggrPlan.tuples
	}

	for _, tuple := range fields {
		if _, err := j.pushSelectExpr(tuple); err != nil {
			return err
		}
	}

	if err := j.handleOthers(); err != nil {
		return err
	}

	j.handleJoinOn()
	return nil
}

// handleOthers used to handle otherLeftJoin|rightNull|otherFilter.
func (j *JoinNode) handleOthers() error {
	var err error
	if err = j.pushNullExprs(); err != nil {
		return err
	}

	if err = j.pushOthers(); err != nil {
		return err
	}

	if lp, ok := j.Left.(*JoinNode); ok {
		if err = lp.handleOthers(); err != nil {
			return err
		}
	}

	if rp, ok := j.Right.(*JoinNode); ok {
		if err = rp.handleOthers(); err != nil {
			return err
		}
	}
	return nil
}

// pushNullExprs used to push rightNull.
func (j *JoinNode) pushNullExprs() error {
	for _, tuple := range j.rightNull {
		index, err := j.pushOtherFilter(j.Right, tuple)
		if err != nil {
			return err
		}
		j.RightTmpCols = append(j.RightTmpCols, index)
	}
	return nil
}

// pushOtherFilters used to push otherFilter.
func (j *JoinNode) pushOtherFilters(filters []exprInfo, isOtherJoin bool) error {
	for _, filter := range filters {
		switch j.Strategy {
		case NestLoop:
			var m *MergeNode
			for _, tb := range filter.referTables {
				tbInfo := j.referTables[tb]
				if m == nil {
					m = tbInfo.parent
					continue
				}
				if m.Order() < tbInfo.parent.Order() {
					m = tbInfo.parent
				}
			}
			m.addWhere(filter.expr)
		case SortMerge:
			var err error
			var lidx, ridx int
			var exchange bool
			exp, _ := filter.expr.(*sqlparser.ComparisonExpr)
			left := parseExpr(exp.Left)
			right := parseExpr(exp.Right)
			ltb := j.Left.getReferTables()
			isLeft := checkTbInNode(left.info.referTables, ltb)

			// exp: t1.a + 1 = t2.a.
			if exp.Operator == sqlparser.EqualStr && (isOtherJoin || !j.IsLeftJoin) &&
				len(left.info.referTables) == 1 && len(right.info.referTables) == 1 {
				if !isLeft {
					exp.Left, exp.Right = exp.Right, exp.Left
					left, right = right, left
				}
				leftKey := j.buildOrderBy(j.Left, left)
				rightKey := j.buildOrderBy(j.Right, right)
				j.LeftKeys = append(j.LeftKeys, leftKey)
				j.RightKeys = append(j.RightKeys, rightKey)
				continue
			}

			if isLeft {
				if lidx, err = j.pushOtherFilter(j.Left, left); err != nil {
					return err
				}
				if ridx, err = j.pushOtherFilter(j.Right, right); err != nil {
					return err
				}
			} else {
				if lidx, err = j.pushOtherFilter(j.Left, right); err != nil {
					return err
				}
				if ridx, err = j.pushOtherFilter(j.Right, left); err != nil {
					return err
				}
				exchange = true
			}

			j.CmpFilter = append(j.CmpFilter, Comparison{lidx, ridx, exp.Operator, exchange})
		}
	}
	return nil
}

// pushOtherFilter used to push otherFilter.
func (j *JoinNode) pushOtherFilter(node PlanNode, tuple selectTuple) (int, error) {
	var err error
	index := -1

	table := tuple.info.referTables[0]
	if tuple.isCol {
		fields := node.getFields()
		for i, field := range fields {
			if field.isCol {
				if table == field.info.referTables[0] && strings.EqualFold(tuple.field, field.field) {
					index = i
					break
				}
			}
		}
	}

	// key not in the select fields.
	if index == -1 {
		if !tuple.isCol {
			tuple.alias = "tmpc"
		}

		index, err = node.pushSelectExpr(tuple)
		if err != nil {
			return index, err
		}
	}
	return index, nil
}

// pushSelectExpr used to push the select field.
func (j *JoinNode) pushSelectExpr(field selectTuple) (int, error) {
	if checkTbInNode(field.info.referTables, j.Left.getReferTables()) {
		index, err := j.Left.pushSelectExpr(field)
		if err != nil {
			return -1, err
		}
		j.Cols = append(j.Cols, -index-1)
	} else {
		if exp, ok := field.expr.(*sqlparser.AliasedExpr); ok && j.IsLeftJoin {
			if _, ok := exp.Expr.(*sqlparser.FuncExpr); ok {
				return -1, errors.Errorf("unsupported: expr.'%s'.in.cross-shard.left.join", field.field)
			}
		}

		if !checkTbInNode(field.info.referTables, j.Right.getReferTables()) {
			if j.Strategy != NestLoop {
				j.setNestLoop()
			}

			// store the origin field name.
			if field.alias == "" {
				field.expr.(*sqlparser.AliasedExpr).As = sqlparser.NewColIdent(field.field)
				field.alias = field.field
			}
		}

		index, err := j.Right.pushSelectExpr(field)
		if err != nil {
			return -1, err
		}
		j.Cols = append(j.Cols, index+1)
	}
	j.fields = append(j.fields, field)
	return len(j.fields) - 1, nil
}

// handleJoinOn used to build order by based on On conditions.
func (j *JoinNode) handleJoinOn() {
	// eg: select t1.a,t2.a from t1 join t2 on t1.a=t2.a;
	// push: select t1.a from t1 order by t1.a asc;
	//       select t2.a from t2 order by t2.a asc;
	if left, ok := j.Left.(*JoinNode); ok {
		left.handleJoinOn()
	}

	if right, ok := j.Right.(*JoinNode); ok {
		right.handleJoinOn()
	}

	for _, join := range j.joinOn {
		var leftKey, rightKey JoinKey
		switch j.Strategy {
		case NestLoop:
			lt := join.cols[0].Qualifier.Name.String()
			rt := join.cols[1].Qualifier.Name.String()
			ltb := j.referTables[lt]
			rtb := j.referTables[rt]

			origin := ltb.parent
			if origin.Order() < rtb.parent.Order() {
				origin = rtb.parent
			}
			origin.addWhere(join.expr)

			leftKey = JoinKey{Field: join.cols[0].Name.String(),
				Table: lt,
			}
			rightKey = JoinKey{Field: join.cols[1].Name.String(),
				Table: rt,
			}
		case SortMerge:
			leftKey = j.buildOrderBy(j.Left, parseExpr(join.cols[0]))
			rightKey = j.buildOrderBy(j.Right, parseExpr(join.cols[1]))
		}

		j.LeftKeys = append(j.LeftKeys, leftKey)
		j.RightKeys = append(j.RightKeys, rightKey)
	}
}

func (j *JoinNode) buildOrderBy(node PlanNode, tuple selectTuple) JoinKey {
	var col *sqlparser.ColName
	index := -1
	table := tuple.info.referTables[0]
	if tuple.isCol {
		fields := node.getFields()
		for i, field := range fields {
			if field.isCol {
				if table == field.info.referTables[0] && strings.EqualFold(tuple.field, field.field) {
					index = i
					break
				}
			}
		}
		col = tuple.info.cols[0]
	}

	// key not in the select fields.
	if index == -1 {
		if !tuple.isCol {
			tuple.alias = "tmpc"
		}

		index, _ = node.pushSelectExpr(tuple)
	}

	if !tuple.isCol {
		col = &sqlparser.ColName{Name: tuple.expr.(*sqlparser.AliasedExpr).As}
	}

	if m, ok := node.(*MergeNode); ok {
		m.Sel.(*sqlparser.Select).OrderBy = append(m.Sel.(*sqlparser.Select).OrderBy, &sqlparser.Order{
			Expr:      col,
			Direction: sqlparser.AscScr,
		})
	}
	return JoinKey{tuple.field, table, index}
}

// pushHaving used to push having expr.
func (j *JoinNode) pushHaving(having exprInfo) error {
	var parent PlanNode
	if len(having.referTables) == 0 {
		j.Left.pushHaving(having)
		j.Right.pushHaving(having)
		return nil
	} else if len(having.referTables) == 1 {
		tbInfo := j.referTables[having.referTables[0]]
		parent = tbInfo.parent
	} else {
		for _, tb := range having.referTables {
			tbInfo := j.referTables[tb]
			if parent == nil {
				parent = tbInfo.parent
				continue
			}
			if parent != tbInfo.parent {
				switch j.Strategy {
				case NestLoop:
					if parent.Order() < tbInfo.parent.Order() {
						parent = tbInfo.parent
					}
				case SortMerge:
					parent = findLCA(j, parent, tbInfo.parent)
				}
			}
		}
	}

	if mn, ok := parent.(*MergeNode); ok {
		mn.addHaving(having.expr)
	} else {
		buf := sqlparser.NewTrackedBuffer(nil)
		having.expr.Format(buf)
		return errors.Errorf("unsupported: havings.'%s'.in.cross-shard.join", buf.String())
	}
	return nil
}

// pushOrderBy used to push the order by exprs.
func (j *JoinNode) pushOrderBy(orderBy sqlparser.OrderBy) error {
	orderPlan := NewOrderByPlan(j.log, orderBy, j)
	j.children = append(j.children, orderPlan)
	return orderPlan.Build()
}

// pushLimit used to push limit.
func (j *JoinNode) pushLimit(limit *sqlparser.Limit) error {
	limitPlan := NewLimitPlan(j.log, limit)
	j.children = append(j.children, limitPlan)
	return limitPlan.Build()
}

// pushMisc used tp push miscelleaneous constructs.
func (j *JoinNode) pushMisc(sel *sqlparser.Select) {
	j.Left.pushMisc(sel)
	j.Right.pushMisc(sel)
}

// Children returns the children of the plan.
func (j *JoinNode) Children() []ChildPlan {
	return j.children
}

// reOrder satisfies the plannode interface.
func (j *JoinNode) reOrder(order int) {
	j.Left.reOrder(order)
	j.Right.reOrder(j.Left.Order())
	j.order = j.Right.Order() + 1
}

// Order satisfies the plannode interface.
func (j *JoinNode) Order() int {
	return j.order
}

// buildQuery used to build the QueryTuple.
func (j *JoinNode) buildQuery(root PlanNode) {
	if j.Strategy == SortMerge {
		if len(j.LeftKeys) == 0 && len(j.CmpFilter) == 0 && !j.IsLeftJoin {
			j.Strategy = Cartesian
		}
	}

	j.Right.addNoTableFilter(j.noTableFilter)
	j.Right.buildQuery(root)

	j.Left.addNoTableFilter(j.noTableFilter)
	j.Left.buildQuery(root)
}

// GetQuery used to get the Querys.
func (j *JoinNode) GetQuery() []xcontext.QueryTuple {
	querys := j.Left.GetQuery()
	querys = append(querys, j.Right.GetQuery()...)
	return querys
}

// procure requests for the specified column from the plan
// and returns the join var name for it.
func (j *JoinNode) procure(col *sqlparser.ColName) string {
	var joinVar string
	field := col.Name.String()
	table := col.Qualifier.Name.String()
	joinVar = col.Qualifier.Name.CompliantName() + "_" + col.Name.CompliantName()
	if _, ok := j.Vars[joinVar]; ok {
		return joinVar
	}

	// `col` must be in `j.Left`.
	tuples := j.Left.getFields()
	index := -1
	for i, tuple := range tuples {
		if tuple.isCol {
			if strings.EqualFold(field, tuple.field) && table == tuple.info.referTables[0] {
				index = i
				break
			}
		}
	}

	// key not in the select fields.
	if index == -1 {
		tuple := selectTuple{
			expr:  &sqlparser.AliasedExpr{Expr: col},
			info:  exprInfo{col, []string{table}, []*sqlparser.ColName{col}, nil},
			field: field,
			isCol: true,
		}
		index, _ = j.Left.pushSelectExpr(tuple)
	}

	j.Vars[joinVar] = index
	return joinVar
}
