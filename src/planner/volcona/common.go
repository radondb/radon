/*
 * Radon
 *
 * Copyright 2021 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package volcona

import "github.com/xelabs/go-mysqlstack/sqlparser"

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

func setParenthese(node Node, hasParen bool) {
	switch node := node.(type) {
	case *Route:
		node.hasParen = hasParen
	case *Join:
		node.hasParen = hasParen
	}
}
