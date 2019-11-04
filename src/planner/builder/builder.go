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
	"xcontext"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// BuildNode used to build the plannode tree.
func BuildNode(log *xlog.Log, router *router.Router, database string, node sqlparser.SelectStatement) (PlanNode, error) {
	var err error
	var root PlanNode
	switch node := node.(type) {
	case *sqlparser.Select:
		root, err = processSelect(log, router, database, node)
	case *sqlparser.Union:
		root, err = processUnion(log, router, database, node)
	default:
		err = errors.New("unsupported: unknown.select.statement")
	}
	if err != nil {
		return nil, err
	}

	root.buildQuery(root.getReferTables())
	return root, nil
}

func processSelect(log *xlog.Log, router *router.Router, database string, node *sqlparser.Select) (PlanNode, error) {
	root, err := scanTableExprs(log, router, database, node.From)
	if err != nil {
		return nil, err
	}

	tbInfos := root.getReferTables()
	if node.Where != nil {
		joins, filters, err := parserWhereOrJoinExprs(node.Where.Expr, tbInfos)
		if err != nil {
			return nil, err
		}
		if err = root.pushFilter(filters); err != nil {
			return nil, err
		}
		root = root.pushEqualCmpr(joins)
	}
	if root, err = root.calcRoute(); err != nil {
		return nil, err
	}

	mn, ok := root.(*MergeNode)
	if ok && mn.routeLen == 1 {
		sel := mn.Sel.(*sqlparser.Select)
		node.From = sel.From
		node.Where = sel.Where
		if err = checkTbName(tbInfos, node); err != nil {
			return nil, err
		}
		mn.Sel = node
		return root, nil
	}

	root.pushMisc(node)

	var groups []selectTuple
	fields, aggTyp, err := parserSelectExprs(node.SelectExprs, root)
	if err != nil {
		return nil, err
	}

	if groups, err = checkGroupBy(node.GroupBy, fields, router, tbInfos, ok); err != nil {
		return nil, err
	}

	if groups, err = checkDistinct(node, groups, fields, router, tbInfos, ok); err != nil {
		return nil, err
	}

	if err = root.pushSelectExprs(fields, groups, node, aggTyp); err != nil {
		return nil, err
	}

	if node.Having != nil {
		havings, err := parserHaving(node.Having.Expr, tbInfos, root.getFields())
		if err != nil {
			return nil, err
		}
		if err = root.pushHaving(havings); err != nil {
			return nil, err
		}
	}

	if err = root.pushOrderBy(node); err != nil {
		return nil, err
	}
	// Limit SubPlan.
	if node.Limit != nil {
		if err = root.pushLimit(node); err != nil {
			return nil, err
		}
	}
	return root, nil
}

// processUnion used to process union.
func processUnion(log *xlog.Log, router *router.Router, database string, node *sqlparser.Union) (PlanNode, error) {
	left, err := processPart(log, router, database, node.Left)
	if err != nil {
		return nil, err
	}
	right, err := processPart(log, router, database, node.Right)
	if err != nil {
		return nil, err
	}

	return union(log, router, database, left, right, node)
}

func processPart(log *xlog.Log, router *router.Router, database string, part sqlparser.SelectStatement) (PlanNode, error) {
	switch part := part.(type) {
	case *sqlparser.Union:
		return processUnion(log, router, database, part)
	case *sqlparser.Select:
		if len(part.OrderBy) > 0 && part.Limit == nil {
			part.OrderBy = []*sqlparser.Order{}
		}
		if len(part.From) == 1 {
			if aliasExpr, ok := part.From[0].(*sqlparser.AliasedTableExpr); ok {
				if tb, ok := aliasExpr.Expr.(sqlparser.TableName); ok && tb.Name.String() == "dual" {
					m := newMergeNode(log, router)
					m.Sel = part
					m.routeLen = 1
					m.nonGlobalCnt = 0
					m.ReqMode = xcontext.ReqSingle
					return m, nil
				}
			}
		}
		node, err := processSelect(log, router, database, part)
		if err != nil {
			return nil, err
		}
		return node, nil
	case *sqlparser.ParenSelect:
		return processPart(log, router, database, part.Select)
	}
	panic(fmt.Sprintf("BUG: unexpected SELECT type: %T", part))
}

// union try to merge the nodes.
func union(log *xlog.Log, router *router.Router, database string, left, right PlanNode, node *sqlparser.Union) (PlanNode, error) {
	if len(left.getFields()) != len(right.getFields()) {
		return nil, errors.New("unsupported: the.used.'select'.statements.have.a.different.number.of.columns")
	}
	lm, lok := left.(*MergeNode)
	rm, rok := right.(*MergeNode)
	if !lok || !rok {
		goto end
	}

	// only single route can merge.
	if lm.routeLen == 1 && rm.routeLen == 1 && (lm.backend == rm.backend || lm.nonGlobalCnt == 0 || rm.nonGlobalCnt == 0) {
		if lm.nonGlobalCnt == 0 && rm.ReqMode != xcontext.ReqSingle {
			lm.backend = rm.backend
			lm.index = rm.index
			lm.ReqMode = rm.ReqMode
		}
		lm.Sel = node
		for k, v := range rm.getReferTables() {
			v.parent = lm
			lm.referTables[k] = v
		}
		return lm, nil
	}
end:
	p := newUnionNode(log, left, right, node.Type)
	if err := p.pushOrderBy(node); err != nil {
		return nil, err
	}
	if err := p.pushLimit(node); err != nil {
		return nil, err
	}
	return p, nil
}
