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
	if where != nil {
		filters := splitAndExpression(nil, where.Expr)
		for _, filter := range filters {
			comparison, ok := filter.(*sqlparser.ComparisonExpr)
			if !ok {
				continue
			}

			// Only deal with Equal statement.
			switch comparison.Operator {
			case sqlparser.EqualStr:
				if nameMatch(comparison.Left, shardkey) {
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

func nameMatch(node sqlparser.Expr, shardkey string) bool {
	colname, ok := node.(*sqlparser.ColName)
	return ok && (colname.Name.String() == shardkey)
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
	if node, ok := node.(*sqlparser.AndExpr); ok {
		filters = splitAndExpression(filters, node.Left)
		return splitAndExpression(filters, node.Right)
	}
	return append(filters, node)
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

type selectTuple struct {
	field    string
	column   string
	fn       string
	distinct bool
}

// parserSelectExpr parses the AliasedExpr to {as, column, func} tuple.
// field:  the filed name of mysql returns
// column: column name
// func:   function name
// For example: select count(*), count(*) as cstar, max(a), max(b) as mb, a as a1, x.b from t,x group by a1,b
// {field:count(*) column:*   fn:count}
// {field:cstar    column:*   fn:count}
// {field:max(a)   column:a   fn:max}
// {field:mb       column:b   fn:max}
// {field:a1       column:a   fn:}
// {field:b        column:x.b fn:}
func parserSelectExpr(expr *sqlparser.AliasedExpr) (*selectTuple, error) {
	field := ""
	colName := ""
	colName1 := ""
	funcName := ""
	distinct := false
	field = expr.As.String()
	switch expr.Expr.(type) {
	case *sqlparser.ColName:
		col := expr.Expr.(*sqlparser.ColName)
		colName = col.Name.String()
		colName1 = colName
		if !col.Qualifier.IsEmpty() {
			colName = col.Qualifier.Name.String() + "." + colName
		}
	case *sqlparser.FuncExpr:
		ex := expr.Expr.(*sqlparser.FuncExpr)
		distinct = ex.Distinct
		funcName = ex.Name.String()
		switch ex.Exprs[0].(type) {
		case *sqlparser.AliasedExpr:
			exx := ex.Exprs[0].(*sqlparser.AliasedExpr)
			tuple, err := parserSelectExpr(exx)
			if err != nil {
				return nil, err
			}
			colName = tuple.column
		case *sqlparser.StarExpr:
			colName = "*"
		}
	}
	if field == "" {
		if funcName != "" {
			field = fmt.Sprintf("%s(%s)", funcName, colName)
		} else {
			field = colName1
		}
	}
	return &selectTuple{field, colName, funcName, distinct}, nil
}

func parserSelectExprs(exprs sqlparser.SelectExprs) ([]selectTuple, error) {
	var tuples []selectTuple
	for _, expr := range exprs {
		switch expr.(type) {
		case *sqlparser.AliasedExpr:
			exp := expr.(*sqlparser.AliasedExpr)
			tuple, err := parserSelectExpr(exp)
			if err != nil {
				return nil, err
			}
			tuples = append(tuples, *tuple)
		case *sqlparser.StarExpr:
			tuple := selectTuple{field: "*", column: "*"}
			tuples = append(tuples, tuple)
		}
	}
	return tuples, nil
}

func checkInTuple(name string, tuples []selectTuple) bool {
	for _, tuple := range tuples {
		if (tuple.field == "*") || (tuple.field == name) {
			return true
		}
	}
	return false
}

// decomposeAvg decomposes avg(a) to sum(a) and count(a).
func decomposeAvg(tuple *selectTuple) []*sqlparser.AliasedExpr {
	var ret []*sqlparser.AliasedExpr
	sum := &sqlparser.AliasedExpr{Expr: &sqlparser.FuncExpr{
		Name:  sqlparser.NewColIdent("sum"),
		Exprs: []sqlparser.SelectExpr{&sqlparser.AliasedExpr{Expr: sqlparser.NewValArg([]byte(tuple.column))}},
	}}
	count := &sqlparser.AliasedExpr{Expr: &sqlparser.FuncExpr{
		Name:  sqlparser.NewColIdent("count"),
		Exprs: []sqlparser.SelectExpr{&sqlparser.AliasedExpr{Expr: sqlparser.NewValArg([]byte(tuple.column))}},
	}}
	ret = append(ret, sum, count)
	return ret
}
