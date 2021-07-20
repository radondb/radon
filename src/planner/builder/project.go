/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

import (
	"backend"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/expression"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/expression/evaluation"
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
	//the field name.
	field string
	// the alias of the field.
	alias       string
	referTables []string
	hasAgg      bool
	isCol       bool
}

// parseSelectExpr parses the AliasedExpr to select tuple.
func parseSelectExpr(expr *sqlparser.AliasedExpr, tables map[string]*tableInfo) (*selectTuple, bool, error) {
	var cols []*sqlparser.ColName
	var referTables []string
	field := ""
	isCol := false
	hasAggr := false
	hasDist := false

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
				if len(tables) == 1 {
					tableName, _ = getOneTableInfo(tables)
				} else {
					return false, errors.Errorf("unsupported: unknown.column.'%s'.in.select.exprs", node.Name.String())
				}
			} else {
				if _, ok := tables[tableName]; !ok {
					return false, errors.Errorf("unsupported: unknown.column.'%s.%s'.in.field.list", tableName, field)
				}
			}

			if isContainKey(referTables, tableName) {
				return true, nil
			}
			referTables = append(referTables, tableName)
		case *sqlparser.FuncExpr:
			if node.IsAggregate() {
				if hasAggr {
					return false, errors.Errorf("unsupported: .more.than.one.aggregate.in.fileds.'%s'", field)
				}
				hasAggr = true
				if node.Distinct {
					hasDist = true
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
		return nil, hasDist, err
	}

	return &selectTuple{expr, field, alias, referTables, hasAggr, isCol}, hasDist, nil
}

func parseSelectExprs(s *backend.Scatter, root PlanNode, tables map[string]*tableInfo, exprs *sqlparser.SelectExprs) ([]selectTuple, aggrType, error) {
	var tuples []selectTuple
	hasAggs := false
	hasDists := false
	_, isMergeNode := root.(*MergeNode)
	i := 0
	for i < len(*exprs) {
		switch exp := (*exprs)[i].(type) {
		case *sqlparser.AliasedExpr:
			tuple, hasDist, err := parseSelectExpr(exp, tables)
			if err != nil {
				return nil, nullAgg, err
			}
			hasAggs = hasAggs || tuple.hasAgg
			hasDists = hasDists || hasDist
			tuples = append(tuples, *tuple)
		case *sqlparser.StarExpr:
			tuple, err := unfoldWildStar(s, exp, tables)
			if err != nil {
				return nil, nullAgg, err
			}
			tuples = append(tuples, tuple...)
			for idx, t := range tuple {
				if idx == 0 {
					(*exprs)[i] = t.expr
					continue
				}
				i++
				*exprs = append(*exprs, &sqlparser.AliasedExpr{})
				copy((*exprs)[(i+1):], (*exprs)[i:])
				(*exprs)[i] = t.expr
			}
		case sqlparser.Nextval:
			return nil, nullAgg, errors.Errorf("unsupported: nextval.in.select.exprs")
		}
		i++
	}

	return tuples, setAggregatorType(hasAggs, hasDists, isMergeNode), nil
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

// checkIsWithNull used to check whether `tb.col is null` or `tb.col<=> null`.
func checkIsWithNull(root PlanNode, filter exprInfo, tbInfos map[string]*tableInfo) (bool, selectTuple) {
	if !checkTbInNode(filter.referTables, tbInfos) {
		return false, selectTuple{}
	}
	if exp, ok := filter.expr.(*sqlparser.IsExpr); ok {
		if exp.Operator == sqlparser.IsNullStr {
			return true, parseExpr(root, exp.Expr)
		}
	}

	if exp, ok := filter.expr.(*sqlparser.ComparisonExpr); ok {
		if exp.Operator == sqlparser.NullSafeEqualStr {
			if _, ok := exp.Left.(*sqlparser.NullVal); ok {
				return true, parseExpr(root, exp.Right)
			}

			if _, ok := exp.Right.(*sqlparser.NullVal); ok {
				return true, parseExpr(root, exp.Left)
			}
		}
	}

	return false, selectTuple{}
}

// parseExpr used to parse the expr to selectTuple.
func parseExpr(root PlanNode, expr sqlparser.Expr) selectTuple {
	tuple := selectTuple{
		expr: &sqlparser.AliasedExpr{Expr: expr},
	}
	sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			tableName := node.Qualifier.Name.String()
			if node == expr {
				tuple.isCol = true
				tuple.field = node.Name.String()
			}
			if isContainKey(tuple.referTables, tableName) {
				return true, nil
			}
			tuple.referTables = append(tuple.referTables, tableName)
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

func extractAggregate(node sqlparser.Expr) (*expression.AggregateExpr, evaluation.Evaluation, error) {
	expr, err := expression.ParseExpression(node)
	if err != nil {
		return nil, nil, err
	}
	if aggr, ok := expr.(*expression.AggregateExpr); ok {
		return aggr, nil, nil
	}

	res := new(expression.AggregateExpr)
	err = expression.Walk(func(plan expression.Expression) (kontinue bool, err error) {
		switch node := plan.(type) {
		case *expression.VariableExpr:
			return false, errors.Errorf("unsupport: contain.aggregate.and.variable.in.select.exprs")
		case *expression.AggregateExpr:
			*res = *node
			expression.ReplaceExpression(expr, node, expression.NewVariableExpr("tmp_aggr", "", ""))
			return false, nil
		}
		return true, nil
	}, expr)
	if err != nil {
		return nil, nil, err
	}

	eval, err := expr.Materialize()
	if err != nil {
		return nil, nil, err
	}
	return res, eval, nil
}

// decomposeAvg decomposes avg(a) to sum(a) and count(a).
func decomposeAvg(alias string, aggr *expression.AggregateExpr) []*sqlparser.AliasedExpr {
	var ret []*sqlparser.AliasedExpr
	sum := &sqlparser.AliasedExpr{
		Expr: &sqlparser.FuncExpr{
			Name:  sqlparser.NewColIdent("sum"),
			Exprs: aggr.Expr.(*sqlparser.FuncExpr).Exprs,
		},
		As: sqlparser.NewColIdent(alias),
	}
	count := &sqlparser.AliasedExpr{Expr: &sqlparser.FuncExpr{
		Name:  sqlparser.NewColIdent("count"),
		Exprs: aggr.Expr.(*sqlparser.FuncExpr).Exprs,
	}}
	ret = append(ret, sum, count)
	return ret
}

// decomposeAgg decomposes the aggregate function.
// such as: avg(a) -> a as `avg(a)`.
func decomposeAgg(alias string, aggr *expression.AggregateExpr) *sqlparser.AliasedExpr {
	var expr sqlparser.Expr
	switch exp := aggr.Expr.(*sqlparser.FuncExpr).Exprs[0].(type) {
	case *sqlparser.StarExpr:
		expr = sqlparser.NewIntVal([]byte("1"))
	case *sqlparser.AliasedExpr:
		expr = exp.Expr
	case sqlparser.Nextval:
		panic("unreachable")
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
		if table == "" && (tuple.field == "*" || strings.EqualFold(field, tuple.alias)) {
			return true, &tuple
		}

		if tuple.field == "*" && (len(tuple.referTables) == 0 || tuple.referTables[0] == table) {
			return true, &tuple
		}

		if tuple.isCol {
			if strings.EqualFold(field, tuple.field) && (table == "" || table == tuple.referTables[0]) {
				return true, &tuple
			}
		}
	}
	return false, nil
}

// checkGroupBy used to check groupby.
func (b *planBuilder) checkGroupBy(exprs sqlparser.GroupBy, fields []selectTuple, canOpt bool) ([]selectTuple, error) {
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
			if _, ok := b.tables[table]; !ok {
				return nil, errors.Errorf("unsupported: unknow.table.in.group.by.field[%s.%s]", table, field)
			}
		}

		for _, tuple := range fields {
			find := false
			if table == "" && strings.EqualFold(field, tuple.alias) {
				find = true
			} else {
				if tuple.isCol {
					if strings.EqualFold(field, tuple.field) && (table == "" || table == tuple.referTables[0]) {
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
			hasShard, err = b.checkShard(table, group.field)
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
func (b *planBuilder) checkDistinct(node *sqlparser.Select, groups, fields []selectTuple, canOpt bool) ([]selectTuple, error) {
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
				ok, err := b.checkShard(tuple.referTables[0], tuple.field)
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

func unfoldWildStar(s *backend.Scatter, expr *sqlparser.StarExpr, tables map[string]*tableInfo) ([]selectTuple, error) {
	var tuples []selectTuple
	if expr.TableName.IsEmpty() {
		for k, v := range tables {
			tuple, err := descTable(s, k, v)
			if err != nil {
				return nil, err
			}
			tuples = append(tuples, tuple...)
		}
		return tuples, nil
	}

	tb := expr.TableName.Name.String()
	db := expr.TableName.Qualifier.String()
	t, ok := tables[tb]
	if !ok {
		return nil, errors.Errorf("unsupported: unknown.table.'%s'.in.field.list", tb)
	}
	if db != "" && db != t.database {
		return nil, errors.Errorf("unsupported: unknown database '%s' in 'field list'", db)
	}
	return descTable(s, tb, t)
}

func descTable(s *backend.Scatter, alias string, t *tableInfo) ([]selectTuple, error) {
	query := fmt.Sprintf("desc %s.%s", t.database, t.tableConfig.Partitions[0].Table)
	txn, err := s.CreateTransaction()
	if err != nil {
		return nil, err
	}
	defer txn.Finish()
	res, err := txn.ExecuteOnThisBackend(t.tableConfig.Partitions[0].Backend, query)
	if err != nil {
		return nil, err
	}
	var tuple []selectTuple
	for _, row := range res.Rows {
		name := row[0].ToString()
		tuple = append(tuple, selectTuple{
			expr: &sqlparser.AliasedExpr{
				Expr: &sqlparser.ColName{
					Name: sqlparser.NewColIdent(name),
					Qualifier: sqlparser.TableName{
						Name: sqlparser.NewTableIdent(alias),
					},
				},
			},
			field:       name,
			referTables: []string{alias},
			isCol:       true,
		})
	}
	return tuple, nil
}
