/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

import (
	"router"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
)

type exprInfo struct {
	// filter expr.
	expr sqlparser.Expr
	// referred tables.
	referTables []string
	// colname in the filter expr.
	cols []*sqlparser.ColName
	// val in the filter expr.
	vals []*sqlparser.SQLVal
}

// parserWhereOrJoinExprs parser exprs in where or join on conditions.
// eg: 't1.a=t2.a and t1.b=2'.
// t1.a=t2.a paser in joins.
// t1.b=2 paser in wheres, t1.b col, 2 val.
func parserWhereOrJoinExprs(exprs sqlparser.Expr, tbInfos map[string]*tableInfo) ([]exprInfo, []exprInfo, error) {
	filters := splitAndExpression(nil, exprs)
	var joins, wheres []exprInfo

	for _, filter := range filters {
		var cols []*sqlparser.ColName
		var vals []*sqlparser.SQLVal
		filter = skipParenthesis(filter)
		filter = convertOrToIn(filter)
		referTables := make([]string, 0, 4)
		err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
			switch node := node.(type) {
			case *sqlparser.ColName:
				cols = append(cols, node)
				tableName := node.Qualifier.Name.String()
				if tableName == "" {
					if len(tbInfos) == 1 {
						tableName, _ = getOneTableInfo(tbInfos)
					} else {
						return false, errors.Errorf("unsupported: unknown.column.'%s'.in.clause", node.Name.String())
					}
				} else {
					if _, ok := tbInfos[tableName]; !ok {
						return false, errors.Errorf("unsupported: unknown.column.'%s.%s'.in.clause", tableName, node.Name.String())
					}
				}

				if isContainKey(tableName, referTables) {
					return true, nil
				}
				referTables = append(referTables, tableName)
			case *sqlparser.Subquery:
				return false, errors.New("unsupported: subqueries.in.select")
			}
			return true, nil
		}, filter)
		if err != nil {
			return nil, nil, err
		}

		condition, ok := filter.(*sqlparser.ComparisonExpr)
		if ok {
			lc, lok := condition.Left.(*sqlparser.ColName)
			rc, rok := condition.Right.(*sqlparser.ColName)
			switch condition.Operator {
			case sqlparser.EqualStr:
				if lok && rok && lc.Qualifier != rc.Qualifier {
					tuple := exprInfo{condition, referTables, []*sqlparser.ColName{lc, rc}, nil}
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
						condition.Left, condition.Right = condition.Right, condition.Left
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
		tuple := exprInfo{filter, referTables, cols, vals}
		wheres = append(wheres, tuple)
	}

	return joins, wheres, nil
}

// GetDMLRouting used to get the routing from the where clause.
func GetDMLRouting(database, table, shardkey string, where *sqlparser.Where, router *router.Router) ([]router.Segment, error) {
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

func nameMatch(node sqlparser.Expr, table, shardkey string) bool {
	colname, ok := node.(*sqlparser.ColName)
	return ok && (colname.Qualifier.Name.String() == "" || colname.Qualifier.Name.String() == table) && (colname.Name.String() == shardkey)
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

// checkJoinOn use to check the join on conditions, according to lpn|rpn to  determine join.cols[0]|cols[1].
// eg: select * from t1 join t2 on t1.a=t2.a join t3 on t2.b=t1.b. 't2.b=t1.b' is forbidden.
func checkJoinOn(lpn, rpn PlanNode, join exprInfo) (exprInfo, error) {
	lt := join.cols[0].Qualifier.Name.String()
	rt := join.cols[1].Qualifier.Name.String()
	if _, ok := lpn.getReferTables()[lt]; ok {
		if _, ok := rpn.getReferTables()[rt]; !ok {
			return join, errors.New("unsupported: join.on.condition.should.cross.left-right.tables")
		}
	} else {
		if _, ok := lpn.getReferTables()[rt]; !ok {
			return join, errors.New("unsupported: join.on.condition.should.cross.left-right.tables")
		}
		join.cols[0], join.cols[1] = join.cols[1], join.cols[0]
	}
	return join, nil
}

// parserHaving used to check the having exprs and parser into tuples.
// unsupport: `select t2.id as tmp, t1.id from t2,t1 having tmp=1`.
func parserHaving(exprs sqlparser.Expr, tbInfos map[string]*tableInfo) ([]exprInfo, error) {
	filters := splitAndExpression(nil, exprs)
	var tuples []exprInfo

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

				if isContainKey(tableName, referTables) {
					return true, nil
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

		tuple := exprInfo{filter, referTables, nil, nil}
		tuples = append(tuples, tuple)
	}

	return tuples, nil
}
