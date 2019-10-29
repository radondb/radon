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

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
)

// getDMLRouting used to get the routing from the where clause.
func getDMLRouting(database, table, shardkey string, where *sqlparser.Where, router *router.Router) ([]router.Segment, error) {
	if shardkey != "" && where != nil {
		filters := splitAndExpression(nil, where.Expr)
		for _, filter := range filters {
			filter = skipParenthesis(filter)
			comparison, ok := filter.(*sqlparser.ComparisonExpr)
			if !ok {
				continue
			}

			// Only deal with Equal statement.
			switch comparison.Operator {
			case sqlparser.EqualStr:
				if nameMatch(comparison.Left, table, shardkey) {
					sqlval, ok := comparison.Right.(*sqlparser.SQLVal)
					if ok {
						return router.Lookup(database, table, sqlval, sqlval)
					}
				}
			}
		}
	}
	return router.Lookup(database, table, nil, nil)
}

func hasSubquery(node sqlparser.SQLNode) bool {
	has := false
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		if _, ok := node.(*sqlparser.Subquery); ok {
			has = true
			return false, errors.New("dummy")
		}
		return true, nil
	}, node)
	return has
}

func checkTbName(tbInfos map[string]*tableInfo, node sqlparser.SQLNode) error {
	return sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		if col, ok := node.(*sqlparser.ColName); ok {
			tableName := col.Qualifier.Name.String()
			if tableName != "" {
				if _, ok := tbInfos[tableName]; !ok {
					buf := sqlparser.NewTrackedBuffer(nil)
					col.Format(buf)
					return false, errors.Errorf("unsupported: unknown.column.'%s'.in.exprs", buf.String())
				}
			}
		}
		return true, nil
	}, node)
}

func nameMatch(node sqlparser.Expr, table, shardkey string) bool {
	colname, ok := node.(*sqlparser.ColName)
	return ok && (colname.Qualifier.Name.String() == "" || colname.Qualifier.Name.String() == table) && (colname.Name.String() == shardkey)
}

// isShardKeyChanging returns true if any of the update
// expressions modify a shardkey column.
func isShardKeyChanging(exprs sqlparser.UpdateExprs, shardkey string) bool {
	if shardkey != "" {
		for _, assignment := range exprs {
			if shardkey == assignment.Name.Name.String() {
				return true
			}
		}
	}
	return false
}

// splitAndExpression breaks up the Expr into AND-separated conditions
// and appends them to filters, which can be shuffled and recombined
// as needed.
func splitAndExpression(filters []sqlparser.Expr, node sqlparser.Expr) []sqlparser.Expr {
	if node == nil {
		return filters
	}
	switch node := node.(type) {
	case *sqlparser.AndExpr:
		filters = splitAndExpression(filters, node.Left)
		return splitAndExpression(filters, node.Right)
	case *sqlparser.ParenExpr:
		if node, ok := node.Expr.(*sqlparser.AndExpr); ok {
			return splitAndExpression(filters, node)
		}
	}
	return append(filters, node)
}

// skipParenthesis skips the parenthesis (if any) of an expression and
// returns the innermost unparenthesized expression.
func skipParenthesis(node sqlparser.Expr) sqlparser.Expr {
	if node, ok := node.(*sqlparser.ParenExpr); ok {
		return skipParenthesis(node.Expr)
	}
	return node
}

// splitOrExpression breaks up the OrExpr into OR-separated conditions.
// Split the Equal conditions into inMap, return the orter conditions.
func splitOrExpression(node sqlparser.Expr, inMap map[*sqlparser.ColName][]sqlparser.Expr) []sqlparser.Expr {
	var subExprs []sqlparser.Expr
	switch expr := node.(type) {
	case *sqlparser.OrExpr:
		subExprs = append(subExprs, splitOrExpression(expr.Left, inMap)...)
		subExprs = append(subExprs, splitOrExpression(expr.Right, inMap)...)
	case *sqlparser.ComparisonExpr:
		canSplit := false
		if expr.Operator == sqlparser.EqualStr {
			if lc, ok := expr.Left.(*sqlparser.ColName); ok {
				if val, ok := expr.Right.(*sqlparser.SQLVal); ok {
					col := checkColInMap(inMap, lc)
					inMap[col] = append(inMap[col], val)
					canSplit = true
				}
			} else {
				if rc, ok := expr.Right.(*sqlparser.ColName); ok {
					if val, ok := expr.Left.(*sqlparser.SQLVal); ok {
						col := checkColInMap(inMap, rc)
						inMap[col] = append(inMap[col], val)
						canSplit = true
					}
				}
			}
		}

		if !canSplit {
			subExprs = append(subExprs, expr)
		}
	case *sqlparser.ParenExpr:
		subExprs = append(subExprs, splitOrExpression(skipParenthesis(expr), inMap)...)
	default:
		subExprs = append(subExprs, expr)
	}
	return subExprs
}

// checkColInMap used to check if the colname is in the map.
func checkColInMap(inMap map[*sqlparser.ColName][]sqlparser.Expr, col *sqlparser.ColName) *sqlparser.ColName {
	for k := range inMap {
		if col.Equal(k) {
			return k
		}
	}
	return col
}

// rebuildOr used to rebuild the OrExpr.
func rebuildOr(node, expr sqlparser.Expr) sqlparser.Expr {
	if node == nil {
		return expr
	}
	return &sqlparser.OrExpr{
		Left:  node,
		Right: expr,
	}
}

// convertOrToIn used to change the EqualStr to InStr.
func convertOrToIn(node sqlparser.Expr) sqlparser.Expr {
	expr, ok := node.(*sqlparser.OrExpr)
	if !ok {
		return node
	}

	inMap := make(map[*sqlparser.ColName][]sqlparser.Expr)
	var result sqlparser.Expr
	subExprs := splitOrExpression(expr, inMap)
	for _, subExpr := range subExprs {
		result = rebuildOr(result, subExpr)
	}
	for k, v := range inMap {
		subExpr := &sqlparser.ComparisonExpr{
			Operator: sqlparser.InStr,
			Left:     k,
			Right:    sqlparser.ValTuple(v),
		}
		result = rebuildOr(result, subExpr)
	}
	return result
}

// checkComparison checks the WHERE or JOIN-ON clause contains non-sqlval comparison(t1.id=t2.id).
func checkComparison(expr sqlparser.Expr) error {
	filters := splitAndExpression(nil, expr)
	for _, filter := range filters {
		comparison, ok := filter.(*sqlparser.ComparisonExpr)
		if !ok {
			continue
		}
		if _, ok := comparison.Right.(*sqlparser.SQLVal); !ok {
			buf := sqlparser.NewTrackedBuffer(nil)
			comparison.Format(buf)
			return errors.Errorf("unsupported: [%s].must.be.value.compare", buf.String())
		}
	}
	return nil
}

// For example: select count(*), count(distinct x.a) as cstar, max(x.a) as mb, t.a as a1, x.b from t,x group by a1,b
// {field:count(*) referTables:{}  aggrFuc:count aggrField:*   distinct:false isCol:false}
// {field:cstar    referTables:{x} aggrFuc:count aggrField:*   distinct:true  isCol:false}
// {field:mb       referTables:{x} aggrFuc:max   aggrField:x.a distinct:false isCol:false}
// {field:a1       referTables:{t} aggrFuc:      isCol:true}
// {field:b        referTables:{x} aggrFuc:      isCol:true}
type selectTuple struct {
	//select expression.
	expr sqlparser.SelectExpr
	//the field name.
	field string
	// the alias of the field.
	alias string
	//the referred tables.
	referTables []string
	//aggregate function name.
	aggrFuc string
	//field in the aggregate function.
	aggrField       string
	distinct, isCol bool
}

// parserSelectExpr parses the AliasedExpr to select tuple.
func parserSelectExpr(expr *sqlparser.AliasedExpr, tbInfos map[string]*tableInfo) (*selectTuple, bool, error) {
	funcName := ""
	field := ""
	aggrField := ""
	distinct := false
	isCol := false
	hasAggregates := false
	referTables := make([]string, 0, 4)

	alias := expr.As.String()
	if col, ok := expr.Expr.(*sqlparser.ColName); ok {
		field = col.Name.String()
		isCol = true
	} else {
		buf := sqlparser.NewTrackedBuffer(nil)
		expr.Expr.Format(buf)
		field = buf.String()
	}

	err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			tableName := node.Qualifier.Name.String()
			if tableName == "" {
				if len(tbInfos) == 1 {
					tableName, _ = getOneTableInfo(tbInfos)
				} else {
					return false, errors.Errorf("unsupported: unknown.column.'%s'.in.select.exprs", node.Name.String())
				}
			} else {
				if _, ok := tbInfos[tableName]; !ok {
					return false, errors.Errorf("unsupported: unknown.column.'%s'.in.field.list", field)
				}
			}
			for _, tb := range referTables {
				if tb == tableName {
					return true, nil
				}
			}
			referTables = append(referTables, tableName)
		case *sqlparser.FuncExpr:
			distinct = node.Distinct
			if node.IsAggregate() {
				hasAggregates = true
				if node != expr.Expr {
					return false, errors.Errorf("unsupported: '%s'.contain.aggregate.in.select.exprs", field)
				}
				funcName = node.Name.String()
				if len(node.Exprs) != 1 {
					return false, errors.Errorf("unsupported: invalid.use.of.group.function[%s]", funcName)
				}
				buf := sqlparser.NewTrackedBuffer(nil)
				node.Exprs.Format(buf)
				aggrField = buf.String()
				if aggrField == "*" && (node.Name.String() != "count" || distinct) {
					return false, errors.Errorf("unsupported: syntax.error.at.'%s'", field)
				}
			}
		case *sqlparser.GroupConcatExpr:
			return false, errors.Errorf("unsupported: group_concat.in.select.exprs")
		case *sqlparser.Subquery:
			return false, errors.Errorf("unsupported: subqueries.in.select.exprs")
		}
		return true, nil
	}, expr.Expr)
	if err != nil {
		return nil, hasAggregates, err
	}

	return &selectTuple{expr, field, alias, referTables, funcName, aggrField, distinct, isCol}, hasAggregates, nil
}

func parserSelectExprs(exprs sqlparser.SelectExprs, root SelectNode) ([]selectTuple, aggrType, error) {
	var tuples []selectTuple
	hasAggs := false
	hasDist := false
	aggType := nullAgg
	tbInfos := root.getReferTables()
	_, isMergeNode := root.(*MergeNode)
	for _, expr := range exprs {
		switch exp := expr.(type) {
		case *sqlparser.AliasedExpr:
			tuple, hasAgg, err := parserSelectExpr(exp, tbInfos)
			if err != nil {
				return nil, aggType, err
			}
			if hasAgg {
				hasAggs = true
				hasDist = hasDist || tuple.distinct
			}
			tuples = append(tuples, *tuple)
		case *sqlparser.StarExpr:
			if !isMergeNode {
				return nil, aggType, errors.New("unsupported: '*'.expression.in.cross-shard.query")
			}
			tuple := selectTuple{expr: exp, field: "*"}
			if !exp.TableName.IsEmpty() {
				tbName := exp.TableName.Name.String()
				if _, ok := tbInfos[tbName]; !ok {
					return nil, aggType, errors.Errorf("unsupported:  unknown.table.'%s'.in.field.list", tbName)
				}
				tuple.referTables = append(tuple.referTables, tbName)
			}

			tuples = append(tuples, tuple)
		case sqlparser.Nextval:
			return nil, aggType, errors.Errorf("unsupported: nextval.in.select.exprs")
		}
	}

	return tuples, setAggregatorType(hasAggs, hasDist, isMergeNode), nil
}

// aggrType mark aggregate function whether can push down.
type aggrType int

const (
	// does not contain an aggregate function.
	nullAgg aggrType = iota
	// aggregate function can push down.
	canPush
	// aggregate function cannot push down.
	notPush
)

// setAggregatorType used to set aggrType.
func setAggregatorType(hasAggr, hasDist, isMergeNode bool) aggrType {
	if hasAggr {
		if hasDist || !isMergeNode {
			return notPush
		}
		return canPush
	}
	return nullAgg
}

// checkTbInNode used to check whether the filter's referTables in the tbInfos.
func checkTbInNode(referTables []string, tbInfos map[string]*tableInfo) bool {
	if len(referTables) == 0 {
		return true
	}
	for _, tb := range referTables {
		if _, ok := tbInfos[tb]; !ok {
			return false
		}
	}
	return true
}

// getTbInExpr used to get the tbs from the expr.
func getTbInExpr(expr sqlparser.Expr) []string {
	var referTables []string
	sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			tableName := node.Qualifier.Name.String()
			for _, tb := range referTables {
				if tb == tableName {
					return true, nil
				}
			}
			referTables = append(referTables, tableName)
		}
		return true, nil
	}, expr)
	return referTables
}

func checkInTuple(field, table string, tuples []selectTuple) (bool, *selectTuple) {
	for _, tuple := range tuples {
		if table == "" && (tuple.field == "*" || field == tuple.alias) {
			return true, &tuple
		}

		if tuple.field == "*" && (len(tuple.referTables) == 0 || tuple.referTables[0] == table) {
			return true, &tuple
		}

		if tuple.isCol {
			if field == tuple.field && (table == "" || table == tuple.referTables[0]) {
				return true, &tuple
			}
		}
	}
	return false, nil
}

// decomposeAvg decomposes avg(a) to sum(a) and count(a).
func decomposeAvg(tuple *selectTuple) []*sqlparser.AliasedExpr {
	var ret []*sqlparser.AliasedExpr
	alias := tuple.alias
	if alias == "" {
		alias = tuple.field
	}
	sum := &sqlparser.AliasedExpr{
		Expr: &sqlparser.FuncExpr{
			Name:  sqlparser.NewColIdent("sum"),
			Exprs: tuple.expr.(*sqlparser.AliasedExpr).Expr.(*sqlparser.FuncExpr).Exprs,
		},
		As: sqlparser.NewColIdent(alias),
	}
	count := &sqlparser.AliasedExpr{Expr: &sqlparser.FuncExpr{
		Name:  sqlparser.NewColIdent("count"),
		Exprs: tuple.expr.(*sqlparser.AliasedExpr).Expr.(*sqlparser.FuncExpr).Exprs,
	}}
	ret = append(ret, sum, count)
	return ret
}

// decomposeAgg decomposes the aggregate function.
// such as: avg(a) -> a as `avg(a)`.
func decomposeAgg(tuple *selectTuple) *sqlparser.AliasedExpr {
	var expr sqlparser.Expr
	switch exp := tuple.expr.(*sqlparser.AliasedExpr).Expr.(*sqlparser.FuncExpr).Exprs[0].(type) {
	case *sqlparser.StarExpr:
		expr = sqlparser.NewIntVal([]byte("1"))
	case *sqlparser.AliasedExpr:
		expr = exp.Expr
	case sqlparser.Nextval:
		panic("unreachable")
	}

	alias := tuple.alias
	if alias == "" {
		alias = tuple.field
	}

	return &sqlparser.AliasedExpr{
		Expr: expr,
		As:   sqlparser.NewColIdent(alias),
	}
}

// convertToLeftJoin converts a right join into a left join.
func convertToLeftJoin(joinExpr *sqlparser.JoinTableExpr) {
	newExpr := joinExpr.LeftExpr
	// If LeftExpr is a join, we have to parenthesize it.
	if _, ok := newExpr.(*sqlparser.JoinTableExpr); ok {
		newExpr = &sqlparser.ParenTableExpr{
			Exprs: sqlparser.TableExprs{newExpr},
		}
	}
	joinExpr.LeftExpr, joinExpr.RightExpr = joinExpr.RightExpr, newExpr
	joinExpr.Join = sqlparser.LeftJoinStr
}

type filterTuple struct {
	// filter expr.
	expr sqlparser.Expr
	// referred tables.
	referTables []string
	// colname in the filter expr.
	col *sqlparser.ColName
	// val in the filter expr.
	vals []*sqlparser.SQLVal
}

type joinTuple struct {
	// join expr.
	expr *sqlparser.ComparisonExpr
	// referred tables.
	referTables []string
	left, right *sqlparser.ColName
}

// parserWhereOrJoinExprs parser exprs in where or join on conditions.
// eg: 't1.a=t2.a and t1.b=2'.
// t1.a=t2.a paser in joinTuple.
// t1.b=2 paser in filterTuple, t1.b col, 2 val.
func parserWhereOrJoinExprs(exprs sqlparser.Expr, tbInfos map[string]*tableInfo) ([]joinTuple, []filterTuple, error) {
	filters := splitAndExpression(nil, exprs)
	var joins []joinTuple
	var wheres []filterTuple

	for _, filter := range filters {
		var col *sqlparser.ColName
		var vals []*sqlparser.SQLVal
		count := 0
		filter = skipParenthesis(filter)
		filter = convertOrToIn(filter)
		referTables := make([]string, 0, 4)
		err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case *sqlparser.ColName:
				count++
				col = node
				tableName := node.Qualifier.Name.String()
				if tableName == "" {
					if len(tbInfos) == 1 {
						tableName, _ = getOneTableInfo(tbInfos)
					} else {
						return false, errors.Errorf("unsupported: unknown.column.'%s'.in.clause", node.Name.String())
					}
				} else {
					if _, ok := tbInfos[tableName]; !ok {
						return false, errors.Errorf("unsupported: unknown.table.'%s'.in.clause", tableName)
					}
				}

				for _, tb := range referTables {
					if tb == tableName {
						return true, nil
					}
				}
				referTables = append(referTables, tableName)
			}
			return true, nil
		}, filter)
		if err != nil {
			return nil, nil, err
		}

		if count != 1 {
			col = nil
		}
		condition, ok := filter.(*sqlparser.ComparisonExpr)
		if ok {
			lc, lok := condition.Left.(*sqlparser.ColName)
			rc, rok := condition.Right.(*sqlparser.ColName)
			switch condition.Operator {
			case sqlparser.EqualStr:
				if lok && rok && lc.Qualifier != rc.Qualifier {
					tuple := joinTuple{condition, referTables, lc, rc}
					joins = append(joins, tuple)
					continue
				}

				if lok {
					if sqlVal, ok := condition.Right.(*sqlparser.SQLVal); ok {
						vals = append(vals, sqlVal)
					}
				}
				if rok {
					if sqlVal, ok := condition.Left.(*sqlparser.SQLVal); ok {
						vals = append(vals, sqlVal)
					}
				}
			case sqlparser.InStr:
				if lok {
					if valTuple, ok := condition.Right.(sqlparser.ValTuple); ok {
						var sqlVals []*sqlparser.SQLVal
						isVal := true
						for _, val := range valTuple {
							if sqlVal, ok := val.(*sqlparser.SQLVal); ok {
								sqlVals = append(sqlVals, sqlVal)
							} else {
								isVal = false
								break
							}
						}
						if isVal {
							vals = sqlVals
						}
					}
				}
			}
		}
		tuple := filterTuple{filter, referTables, col, vals}
		wheres = append(wheres, tuple)
	}

	return joins, wheres, nil
}

// checkJoinOn use to check the join on conditions, according to lpn|rpn to  determine join.left|right.
// eg: select * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t1.b. 't2.b=t1.b' is forbidden.
func checkJoinOn(lpn, rpn SelectNode, join joinTuple) (joinTuple, error) {
	lt := join.left.Qualifier.Name.String()
	rt := join.right.Qualifier.Name.String()
	if _, ok := lpn.getReferTables()[lt]; ok {
		if _, ok := rpn.getReferTables()[rt]; !ok {
			return join, errors.New("unsupported: join.on.condition.should.cross.left-right.tables")
		}
	} else {
		if _, ok := lpn.getReferTables()[rt]; !ok {
			return join, errors.New("unsupported: join.on.condition.should.cross.left-right.tables")
		}
		join.left, join.right = join.right, join.left
	}
	return join, nil
}

// checkGroupBy used to check groupby.
func checkGroupBy(exprs sqlparser.GroupBy, fields []selectTuple, router *router.Router, tbInfos map[string]*tableInfo, canOpt bool) ([]selectTuple, error) {
	var groupTuples []selectTuple
	hasShard := false
	for _, expr := range exprs {
		var group *selectTuple
		// TODO: support group by 1,2.
		col, ok := expr.(*sqlparser.ColName)
		if !ok {
			buf := sqlparser.NewTrackedBuffer(nil)
			expr.Format(buf)
			return nil, errors.Errorf("unsupported: group.by.[%s].type.should.be.colname", buf.String())
		}
		field := col.Name.String()
		table := col.Qualifier.Name.String()
		if table != "" {
			if _, ok := tbInfos[table]; !ok {
				return nil, errors.Errorf("unsupported: unknow.table.in.group.by.field[%s.%s]", table, field)
			}
		}

		for _, tuple := range fields {
			find := false
			if table == "" && field == tuple.alias {
				find = true
			} else {
				if tuple.isCol {
					if field == tuple.field && (table == "" || table == tuple.referTables[0]) {
						find = true
					}
				}
			}
			if find {
				group = &tuple
				groupTuples = append(groupTuples, *group)
				break
			}
		}
		if group == nil {
			if table != "" {
				field = fmt.Sprintf("%s.%s", table, field)
			}
			return nil, errors.Errorf("unsupported: group.by.field[%s].should.be.in.select.list", field)
		}
		if canOpt && group.isCol && !hasShard {
			table = group.referTables[0]
			var err error
			// If fields contains shardkey, just push down the group by,
			// neednot process groupby again. unsupport alias.
			hasShard, err = checkShard(table, group.field, tbInfos, router)
			if err != nil {
				return nil, err
			}
		}
	}

	if hasShard {
		return nil, nil
	}
	return groupTuples, nil
}

// checkDistinct used to check the distinct, and convert distinct to groupby.
func checkDistinct(node *sqlparser.Select, groups, fields []selectTuple, router *router.Router, tbInfos map[string]*tableInfo, canOpt bool) ([]selectTuple, error) {
	// field in grouby must be contained in the select exprs, that mains groups is a subset of fields.
	// if has groupby, neednot process distinct again.
	if node.Distinct == "" || len(node.GroupBy) > 0 {
		return groups, nil
	}

	// If fields contains shardkey, just push down group by,
	// neednot process distinct again.
	hasShard := false
	if canOpt {
		for _, tuple := range fields {
			if tuple.isCol {
				ok, err := checkShard(tuple.referTables[0], tuple.field, tbInfos, router)
				if err != nil {
					return nil, err
				}
				if ok {
					hasShard = true
					break
				}
			}
		}
	}

	// distinct convert to groupby.
	for _, tuple := range fields {
		expr, ok := tuple.expr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, errors.New("unsupported: distinct")
		}
		if expr.As.IsEmpty() {
			if _, ok := expr.Expr.(*sqlparser.ColName); !ok {
				return nil, errors.New("unsupported: distinct")
			}
			node.GroupBy = append(node.GroupBy, expr.Expr)
		} else {
			node.GroupBy = append(node.GroupBy, &sqlparser.ColName{
				Name: expr.As,
			})
		}
	}
	node.Distinct = ""
	if hasShard {
		return nil, nil
	}
	return fields, nil
}

// parserHaving used to check the having exprs and parser into tuples.
// unsupport: `select t2.id as tmp, t1.id from t2,t1 having tmp=1`.
func parserHaving(exprs sqlparser.Expr, tbInfos map[string]*tableInfo) ([]filterTuple, error) {
	filters := splitAndExpression(nil, exprs)
	var tuples []filterTuple

	for _, filter := range filters {
		filter = skipParenthesis(filter)
		referTables := make([]string, 0, 4)
		err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case *sqlparser.ColName:
				tableName := node.Qualifier.Name.String()
				if tableName == "" {
					if len(tbInfos) == 1 {
						tableName, _ = getOneTableInfo(tbInfos)
					} else {
						return false, errors.Errorf("unsupported: unknown.column.'%s'.in.having.clause", node.Name.String())
					}
				} else {
					if _, ok := tbInfos[tableName]; !ok {
						return false, errors.Errorf("unsupported: unknown.table.'%s'.in.having.clause", tableName)
					}
				}
				for _, tb := range referTables {
					if tb == tableName {
						return true, nil
					}
				}
				referTables = append(referTables, tableName)
			case *sqlparser.FuncExpr:
				if node.IsAggregate() {
					buf := sqlparser.NewTrackedBuffer(nil)
					node.Format(buf)
					return false, errors.Errorf("unsupported: expr[%s].in.having.clause", buf.String())
				}
			}
			return true, nil
		}, filter)
		if err != nil {
			return nil, err
		}

		tuple := filterTuple{filter, referTables, nil, nil}
		tuples = append(tuples, tuple)
	}

	return tuples, nil
}

type nullExpr struct {
	expr sqlparser.Expr
	// referred tables.
	referTables []string
}

// checkIsWithNull used to check whether `tb.col is null` or `tb.col<=> null`.
func checkIsWithNull(filter filterTuple, tbInfos map[string]*tableInfo) (bool, nullExpr) {
	if !checkTbInNode(filter.referTables, tbInfos) {
		return false, nullExpr{}
	}
	if exp, ok := filter.expr.(*sqlparser.IsExpr); ok {
		if exp.Operator == sqlparser.IsNullStr {
			return true, nullExpr{exp.Expr, filter.referTables}
		}
	}

	if exp, ok := filter.expr.(*sqlparser.ComparisonExpr); ok {
		if exp.Operator == sqlparser.NullSafeEqualStr {
			if _, ok := exp.Left.(*sqlparser.NullVal); ok {
				return true, nullExpr{exp.Right, filter.referTables}
			}

			if _, ok := exp.Right.(*sqlparser.NullVal); ok {
				return true, nullExpr{exp.Left, filter.referTables}
			}
		}
	}

	return false, nullExpr{}
}

// checkShard used to check whether the col is shardkey.
func checkShard(table, col string, tbInfos map[string]*tableInfo, router *router.Router) (bool, error) {
	tbInfo, ok := tbInfos[table]
	if !ok {
		return false, errors.Errorf("unsupported: unknown.column.'%s.%s'.in.field.list", table, col)
	}

	shardkey, err := router.ShardKey(tbInfo.database, tbInfo.tableName)
	if err != nil {
		return false, err
	}
	if shardkey == col {
		return true, nil
	}
	return false, nil
}

// getIndex used to get index from router.
func getIndex(router *router.Router, tbInfo *tableInfo, val *sqlparser.SQLVal) error {
	idx, err := router.GetIndex(tbInfo.database, tbInfo.tableName, val)
	if err != nil {
		return err
	}

	tbInfo.parent.index = append(tbInfo.parent.index, idx)
	return nil
}

func getSelectExprs(node sqlparser.SelectStatement) sqlparser.SelectExprs {
	var exprs sqlparser.SelectExprs
	switch node := node.(type) {
	case *sqlparser.Select:
		exprs = node.SelectExprs
	case *sqlparser.Union:
		exprs = getSelectExprs(node.Left)
	}
	return exprs
}
