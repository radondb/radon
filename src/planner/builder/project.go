/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

import (
	"fmt"

	"router"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
)

// For example: select count(*), count(distinct x.a) as cstar, max(x.a) as mb, t.a as a1, x.b from t,x group by a1,b
// {field:count(*)            info.referTables:{}  aggrFuc:count aggrField:*   distinct:false isCol:false}
// {field:count(distinct x.a) info.referTables:{x} aggrFuc:count aggrField:*   distinct:true  isCol:false  alias:cstar}
// {field:max(x.a)            info.referTables:{x} aggrFuc:max   aggrField:x.a distinct:false isCol:false  alias:mb }
// {field:a                   info.referTables:{t} isCol:true    alias:a1}
// {field:b                   info.referTables:{x} isCol:true}
type selectTuple struct {
	//select expression.
	expr sqlparser.SelectExpr
	info exprInfo
	//the field name.
	field string
	// the alias of the field.
	alias string
	//aggregate function name.
	aggrFuc string
	//field in the aggregate function.
	aggrField       string
	distinct, isCol bool
}

// parserSelectExpr parses the AliasedExpr to select tuple.
func parserSelectExpr(expr *sqlparser.AliasedExpr, tbInfos map[string]*tableInfo) (*selectTuple, bool, error) {
	var cols []*sqlparser.ColName
	var referTables []string
	funcName := ""
	field := ""
	aggrField := ""
	distinct := false
	isCol := false
	hasAggregates := false

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
			cols = append(cols, node)
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

			if isContainKey(tableName, referTables) {
				return true, nil
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

	return &selectTuple{expr, exprInfo{expr.Expr, referTables, cols, nil}, field, alias, funcName, aggrField, distinct, isCol}, hasAggregates, nil
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
				tuple.info.referTables = append(tuple.info.referTables, tbName)
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

type nullExpr struct {
	expr sqlparser.Expr
	// referred tables.
	referTables []string
}

// checkIsWithNull used to check whether `tb.col is null` or `tb.col<=> null`.
func checkIsWithNull(filter exprInfo, tbInfos map[string]*tableInfo) (bool, selectTuple) {
	if !checkTbInNode(filter.referTables, tbInfos) {
		return false, selectTuple{}
	}
	if exp, ok := filter.expr.(*sqlparser.IsExpr); ok {
		if exp.Operator == sqlparser.IsNullStr {
			return true, parserExpr(exp.Expr)
		}
	}

	if exp, ok := filter.expr.(*sqlparser.ComparisonExpr); ok {
		if exp.Operator == sqlparser.NullSafeEqualStr {
			if _, ok := exp.Left.(*sqlparser.NullVal); ok {
				return true, parserExpr(exp.Right)
			}

			if _, ok := exp.Right.(*sqlparser.NullVal); ok {
				return true, parserExpr(exp.Left)
			}
		}
	}

	return false, selectTuple{}
}

// parserExpr used to parser the expr to selectTuple.
func parserExpr(expr sqlparser.Expr) selectTuple {
	tuple := selectTuple{expr: &sqlparser.AliasedExpr{Expr: expr}, info: exprInfo{expr: expr}}
	sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			tableName := node.Qualifier.Name.String()
			if node == expr {
				tuple.isCol = true
				tuple.field = node.Name.String()
			}
			tuple.info.cols = append(tuple.info.cols, node)
			if isContainKey(tableName, tuple.info.referTables) {
				return true, nil
			}
			tuple.info.referTables = append(tuple.info.referTables, tableName)
		}
		return true, nil
	}, expr)

	if tuple.field == "" {
		buf := sqlparser.NewTrackedBuffer(nil)
		expr.Format(buf)
		tuple.field = buf.String()
	}
	return tuple
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

func checkInTuple(field, table string, tuples []selectTuple) (bool, *selectTuple) {
	for _, tuple := range tuples {
		if table == "" && (tuple.field == "*" || field == tuple.alias) {
			return true, &tuple
		}

		if tuple.field == "*" && (len(tuple.info.referTables) == 0 || tuple.info.referTables[0] == table) {
			return true, &tuple
		}

		if tuple.isCol {
			if field == tuple.field && (table == "" || table == tuple.info.referTables[0]) {
				return true, &tuple
			}
		}
	}
	return false, nil
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
					if field == tuple.field && (table == "" || table == tuple.info.referTables[0]) {
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
			table = group.info.referTables[0]
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
				ok, err := checkShard(tuple.info.referTables[0], tuple.field, tbInfos, router)
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

// GetProject return the project which is used in explain.
func GetProject(root PlanNode) string {
	var prefix, project string
	tuples := root.getFields()
	for _, tuple := range tuples {
		field := tuple.field
		if tuple.alias != "" {
			field = tuple.alias
		}
		project = fmt.Sprintf("%s%s%s", project, prefix, field)
		prefix = ", "
	}
	return project
}
