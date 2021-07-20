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

	"router"
	"xcontext"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

type planBuilder struct {
	log      *xlog.Log
	router   *router.Router
	scatter  *backend.Scatter
	database string
	tables   map[string]*tableInfo
	root     PlanNode
}

func NewPlanBuilder(log *xlog.Log, router *router.Router, scatter *backend.Scatter, database string) *planBuilder {
	return &planBuilder{
		log:      log,
		router:   router,
		scatter:  scatter,
		tables:   make(map[string]*tableInfo),
		database: database,
	}
}

// BuildNode used to build the plannode tree.
func BuildNode(log *xlog.Log, router *router.Router, scatter *backend.Scatter, database string, node sqlparser.SelectStatement) (PlanNode, error) {
	var err error
	var root PlanNode
	b := NewPlanBuilder(log, router, scatter, database)
	switch node := node.(type) {
	case *sqlparser.Select:
		root, err = b.processSelect(node)
	case *sqlparser.Union:
		root, err = b.processUnion(node)
	default:
		err = errors.New("unsupported: unknown.select.statement")
	}
	if err != nil {
		return nil, err
	}

	root.buildQuery(root)
	return root, nil
}

func (b *planBuilder) processSelect(node *sqlparser.Select) (PlanNode, error) {
	var err error
	b.root, err = b.scanTableExprs(node.From)
	if err != nil {
		return nil, err
	}

	if node.Where != nil {
		if err = b.pushFilters(node.Where.Expr); err != nil {
			return nil, err
		}
	}
	if b.root, err = b.root.calcRoute(); err != nil {
		return nil, err
	}

	mn, ok := b.root.(*MergeNode)
	if ok && mn.routeLen == 1 {
		sel := mn.Sel.(*sqlparser.Select)
		node.From = sel.From
		node.Where = sel.Where
		if err = checkTbName(b.tables, node); err != nil {
			return nil, err
		}
		mn.Sel = node
		return b.root, nil
	}

	var groups []selectTuple
	fields, aggTyp, err := parseSelectExprs(b.scatter, b.root, b.root.getReferTables(), &node.SelectExprs)
	if err != nil {
		return nil, err
	}

	if groups, err = b.checkGroupBy(node.GroupBy, fields, ok); err != nil {
		return nil, err
	}

	if groups, err = b.checkDistinct(node, groups, fields, ok); err != nil {
		return nil, err
	}

	if err = b.root.pushSelectExprs(fields, groups, node, aggTyp); err != nil {
		return nil, err
	}

	if node.Having != nil {
		if err = b.pushHavings(node.Having.Expr); err != nil {
			return nil, err
		}
	}

	if len(node.OrderBy) > 0 {
		if err = b.root.pushOrderBy(node.OrderBy); err != nil {
			return nil, err
		}
	}

	// Limit SubPlan.
	if node.Limit != nil {
		if err = b.root.pushLimit(node.Limit); err != nil {
			return nil, err
		}
	}

	b.root.pushMisc(node)
	return b.root, nil
}

// processUnion used to process union.
func (b *planBuilder) processUnion(node *sqlparser.Union) (PlanNode, error) {
	left, err := b.processPart(node.Left)
	if err != nil {
		return nil, err
	}
	right, err := b.processPart(node.Right)
	if err != nil {
		return nil, err
	}

	return b.union(left, right, node)
}

func (b *planBuilder) processPart(part sqlparser.SelectStatement) (PlanNode, error) {
	switch part := part.(type) {
	case *sqlparser.Union:
		return b.processUnion(part)
	case *sqlparser.Select:
		if len(part.From) == 1 {
			if aliasExpr, ok := part.From[0].(*sqlparser.AliasedTableExpr); ok {
				if tb, ok := aliasExpr.Expr.(sqlparser.TableName); ok && tb.Name.String() == "dual" {
					m := newMergeNode(b.log, b.router, b.scatter)
					m.Sel = part
					m.routeLen = 1
					m.nonGlobalCnt = 0
					m.ReqMode = xcontext.ReqSingle
					return m, nil
				}
			}
		}
		node, err := b.processSelect(part)
		if err != nil {
			return nil, err
		}
		return node, nil
	case *sqlparser.ParenSelect:
		return b.processPart(part.Select)
	}
	panic(fmt.Sprintf("BUG: unexpected SELECT type: %T", part))
}

// union try to merge the nodes.
func (b *planBuilder) union(left, right PlanNode, node *sqlparser.Union) (PlanNode, error) {
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
			lm.indexes = rm.indexes
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
	p := newUnionNode(b.log, left, right, node.Type)
	if len(node.OrderBy) > 0 {
		if err := p.pushOrderBy(node.OrderBy); err != nil {
			return nil, err
		}
	}
	if node.Limit != nil {
		if err := p.pushLimit(node.Limit); err != nil {
			return nil, err
		}
	}
	return p, nil
}
