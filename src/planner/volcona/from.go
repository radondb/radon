/*
 * Radon
 *
 * Copyright 2021 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package volcona

import (
	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
)

func (b *planBuilder) parseTableExprs(exprs sqlparser.TableExprs) (Node, error) {
	if len(exprs) == 1 {
		return b.parseTableExpr(exprs[0])
	}

	var lpn, rpn Node
	var err error
	if lpn, err = b.parseTableExpr(exprs[0]); err != nil {
		return nil, err
	}
	if rpn, err = b.parseTableExprs(exprs[1:]); err != nil {
		return nil, err
	}
	return join(lpn, rpn, nil)
}

func (b *planBuilder) parseTableExpr(expr sqlparser.TableExpr) (Node, error) {
	var err error
	var p Node
	switch expr := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		p, err = b.parseAliasedTableExpr(expr)
	case *sqlparser.JoinTableExpr:
		p, err = b.parseJoinTableExpr(expr)
	case *sqlparser.ParenTableExpr:
		p, err = b.parseTableExprs(expr.Exprs)
		// If finally p is a Route, the pushed query need keep the parenthese.
		setParenthese(p, true)
	}
	return p, err
}

func (b *planBuilder) parseAliasedTableExpr(aliased *sqlparser.AliasedTableExpr) (Node, error) {
	var err error
	switch simple := aliased.Expr.(type) {
	case sqlparser.TableName:
		r := &Route{
			Stmt: &sqlparser.Select{From: sqlparser.TableExprs([]sqlparser.TableExpr{aliased})},
		}

		tbName := simple.Name.String()
		if tbName == "dual" {
			r.isDual = true
			return r, nil
		}

		if simple.Qualifier.IsEmpty() {
			simple.Qualifier = sqlparser.NewTableIdent(b.database)
		}
		tb := &tableInfo{
			database: simple.Qualifier.String(),
			name:     simple.Name.String(),
		}
		tb.tableConf, err = b.router.TableConfig(tb.database, tb.name)
		if err != nil {
			return nil, err
		}
		tb.tableExpr = aliased

		switch tb.tableConf.ShardType {
		case "GLOBAL":
			r.isGlobal = true
		case "SINGLE":
			r.indexes = append(r.indexes, 0)
		case "HASH", "LIST":
			// if a shard table hasn't alias, create one in order to push.
			if aliased.As.String() == "" {
				aliased.As = sqlparser.NewTableIdent(tb.name)
			}
		}

		tb.parent = r
		tb.alias = aliased.As.String()
		if tb.alias != "" {
			r.referTables[tb.alias] = tb
		} else {
			r.referTables[tb.name] = tb
		}
		return r, nil
	case *sqlparser.Subquery:

	}
	return nil, nil
}

func (b *planBuilder) parseJoinTableExpr(joinExpr *sqlparser.JoinTableExpr) (Node, error) {
	switch joinExpr.Join {
	case sqlparser.JoinStr, sqlparser.StraightJoinStr, sqlparser.LeftJoinStr:
	case sqlparser.RightJoinStr:
		convertToLeftJoin(joinExpr)
	default:
		return nil, errors.Errorf("unsupported: join.type:%s", joinExpr.Join)
	}
	lpn, err := b.parseTableExpr(joinExpr.LeftExpr)
	if err != nil {
		return nil, err
	}

	rpn, err := b.parseTableExpr(joinExpr.RightExpr)
	if err != nil {
		return nil, err
	}
	return join(lpn, rpn, joinExpr)
}

func join(lpn, rpn Node, joinExpr *sqlparser.JoinTableExpr) (Node, error) {
	return nil, nil
}
