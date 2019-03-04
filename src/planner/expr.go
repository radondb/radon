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

func nameMatch(node sqlparser.Expr, table, shardkey string) bool {
	colname, ok := node.(*sqlparser.ColName)
	return ok && (colname.Qualifier.Name.String() == "" || colname.Qualifier.Name.String() == table) && (colname.Name.String() == shardkey)
}

// isShardKeyChanging returns true if any of the update
// expressions modify a shardkey column.
func isShardKeyChanging(exprs sqlparser.UpdateExprs, shardkey string) bool {
	for _, assignment := range exprs {
		if shardkey == assignment.Name.Name.String() {
			return true
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
// {field:count(*) referTables:{}   aggrFuc:count  aggrField:*   distinct:false}
// {field:cstar    referTables:{x}  aggrFuc:count  aggrField:*   distinct:true}
// {field:mb       referTables:{x}  aggrFuc:max    aggrField:x.a distinct:false}
// {field:a1       referTables:{t}  aggrFuc:}
// {field:b      referTables:{x}  aggrFuc:}
type selectTuple struct {
	//select expression
	expr sqlparser.SelectExpr
	//the field name of mysql returns
	field string
	//the referred tables
	referTables []string
	//aggregate function name
	aggrFuc string
	//field in the aggregate function
	aggrField string
	distinct  bool
}

// parserSelectExpr parses the AliasedExpr to select tuple.
func parserSelectExpr(expr *sqlparser.AliasedExpr, tbInfos map[string]*TableInfo) (*selectTuple, bool, error) {
	funcName := ""
	aggrField := ""
	distinct := false
	hasAggregates := false
	referTables := make([]string, 0, 4)

	field := expr.As.String()
	if field == "" {
		if col, ok := expr.Expr.(*sqlparser.ColName); ok {
			field = col.Name.String()
		} else {
			buf := sqlparser.NewTrackedBuffer(nil)
			expr.Format(buf)
			field = buf.String()
		}
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
				if aggrField == "*" && node.Name.String() != "count" {
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

	return &selectTuple{expr, field, referTables, funcName, aggrField, distinct}, hasAggregates, nil
}

func parserSelectExprs(exprs sqlparser.SelectExprs, root PlanNode) ([]selectTuple, bool, error) {
	var tuples []selectTuple
	hasAggregates := false
	tbInfos := root.getReferredTables()
	for _, expr := range exprs {
		switch exp := expr.(type) {
		case *sqlparser.AliasedExpr:
			tuple, hasAggregate, err := parserSelectExpr(exp, tbInfos)
			if err != nil {
				return nil, false, err
			}
			if hasAggregate {
				hasAggregates = true
			}
			tuples = append(tuples, *tuple)
		case *sqlparser.StarExpr:
			if _, ok := root.(*MergeNode); !ok {
				return nil, false, errors.New("unsupported: '*'.expression.in.cross-shard.query")
			}
			tuple := selectTuple{expr: exp, field: "*"}
			if !exp.TableName.IsEmpty() {
				tbName := exp.TableName.Name.String()
				if _, ok := tbInfos[tbName]; !ok {
					return nil, false, errors.Errorf("unsupported:  unknown.table.'%s'.in.field.list", tbName)
				}
				tuple.referTables = append(tuple.referTables, tbName)
			}

			tuples = append(tuples, tuple)
		case sqlparser.Nextval:
			return nil, false, errors.Errorf("unsupported: nextval.in.select.exprs")
		}
	}
	return tuples, hasAggregates, nil
}

func checkInTuple(field, table string, tuples []selectTuple) bool {
	for _, tuple := range tuples {
		if tuple.field == "*" || tuple.field == field {
			if table == "" || len(tuple.referTables) == 0 {
				return true
			}
			if len(tuple.referTables) == 1 && tuple.referTables[0] == table {
				return true
			}
		}
	}
	return false
}

// decomposeAvg decomposes avg(a) to sum(a) and count(a).
func decomposeAvg(tuple *selectTuple) []*sqlparser.AliasedExpr {
	var ret []*sqlparser.AliasedExpr
	sum := &sqlparser.AliasedExpr{
		Expr: &sqlparser.FuncExpr{
			Name:  sqlparser.NewColIdent("sum"),
			Exprs: []sqlparser.SelectExpr{&sqlparser.AliasedExpr{Expr: sqlparser.NewValArg([]byte(tuple.aggrField))}},
		},
		As: sqlparser.NewColIdent(tuple.field),
	}
	count := &sqlparser.AliasedExpr{Expr: &sqlparser.FuncExpr{
		Name:  sqlparser.NewColIdent("count"),
		Exprs: []sqlparser.SelectExpr{&sqlparser.AliasedExpr{Expr: sqlparser.NewValArg([]byte(tuple.aggrField))}},
	}}
	ret = append(ret, sum, count)
	return ret
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
	val sqlparser.Expr
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
func parserWhereOrJoinExprs(exprs sqlparser.Expr, tbInfos map[string]*TableInfo) ([]joinTuple, []filterTuple, error) {
	filters := splitAndExpression(nil, exprs)
	var joins []joinTuple
	var wheres []filterTuple

	for _, filter := range filters {
		var col *sqlparser.ColName
		var val sqlparser.Expr
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

		condition, ok := filter.(*sqlparser.ComparisonExpr)
		if ok {
			if condition.Operator == sqlparser.EqualStr {
				lc, lok := condition.Left.(*sqlparser.ColName)
				rc, rok := condition.Right.(*sqlparser.ColName)
				if lok && rok && lc.Qualifier != rc.Qualifier {
					tuple := joinTuple{condition, referTables, lc, rc}
					joins = append(joins, tuple)
					continue
				}

				if lok {
					col = lc
					val = condition.Right
				}
				if rok {
					col = rc
					val = condition.Left
				}
			}
		}
		tuple := filterTuple{filter, referTables, col, val}
		wheres = append(wheres, tuple)
	}

	return joins, wheres, nil
}

// checkJoinOn use to check the join on conditions, according to lpn|rpn to  determine join.left|right.
// eg: select * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t1.b. 't2.b=t1.b' is forbidden.
func checkJoinOn(lpn, rpn PlanNode, join joinTuple) (joinTuple, error) {
	lt := join.left.Qualifier.Name.String()
	rt := join.right.Qualifier.Name.String()
	if _, ok := lpn.getReferredTables()[lt]; ok {
		if _, ok := rpn.getReferredTables()[rt]; !ok {
			return join, errors.New("unsupported: join.on.condition.should.cross.left-right.tables")
		}
	} else {
		if _, ok := lpn.getReferredTables()[rt]; !ok {
			return join, errors.New("unsupported: join.on.condition.should.cross.left-right.tables")
		}
		join.left, join.right = join.right, join.left
	}
	return join, nil
}

// checkGroupBy used to check groupby.
func checkGroupBy(exprs sqlparser.GroupBy, fileds []selectTuple, router *router.Router, tbInfos map[string]*TableInfo) ([]selectTuple, error) {
	var groupTuples []selectTuple
	for _, expr := range exprs {
		var group *selectTuple
		// TODO: support group by 1,2.
		col, ok := expr.(*sqlparser.ColName)
		if !ok {
			return nil, errors.New("unsupported: group.by.field.have.expression")
		}
		field := col.Name.String()
		table := col.Qualifier.Name.String()
		if table != "" {
			if _, ok := tbInfos[table]; !ok {
				return nil, errors.Errorf("unsupported: unknow.table.in.group.by.field[%s.%s]", table, field)
			}
		}

		for _, tuple := range fileds {
			if tuple.field == field && (table == "" || len(tuple.referTables) == 1 && tuple.referTables[0] == table) {
				group = &tuple
				groupTuples = append(groupTuples, *group)
				break
			}
		}
		if group == nil {
			return nil, errors.Errorf("unsupported: group.by.field[%s].should.be.in.select.list", field)
		}
		if len(group.referTables) != 1 {
			continue
		}
		table = group.referTables[0]

		// shardkey is a unique constraints key. if fileds contains shardkey,
		// that mains each row of data is unique. neednot process groupby again.
		// unsupport alias.
		ok, err := checkShard(table, col.Name.String(), tbInfos, router)
		if err != nil {
			return nil, err
		}
		if ok {
			return nil, nil
		}
	}

	return groupTuples, nil
}

// checkDistinct used to check the distinct, and convert distinct to groupby.
func checkDistinct(node *sqlparser.Select, groups, fileds []selectTuple, router *router.Router, tbInfos map[string]*TableInfo) ([]selectTuple, error) {
	// field in grouby must be contained in the select exprs, that mains groups is a subset of fields.
	// if has groupby, neednot process distinct again.
	if node.Distinct == "" || len(node.GroupBy) > 0 {
		return groups, nil
	}

	// shardkey is a unique constraints key. if fileds contains shardkey,
	// that mains each row of data is unique. neednot process distinct again.
	for _, tuple := range fileds {
		if expr, ok := tuple.expr.(*sqlparser.AliasedExpr); ok {
			if exp, ok := expr.Expr.(*sqlparser.ColName); ok {
				ok, err := checkShard(tuple.referTables[0], exp.Name.String(), tbInfos, router)
				if err != nil {
					return nil, err
				}
				if ok {
					return groups, nil
				}
			}
		}
	}

	// distinct convert to groupby.
	for _, tuple := range fileds {
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
	return fileds, nil
}

// checkShard used to check whether the col is shardkey.
func checkShard(table, col string, tbInfos map[string]*TableInfo, router *router.Router) (bool, error) {
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

// parserHaving used to check the having exprs and parser into tuples.
func parserHaving(exprs sqlparser.Expr, tbInfos map[string]*TableInfo) ([]filterTuple, error) {
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
