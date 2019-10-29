/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"encoding/json"
	"fmt"

	"router"
	"xcontext"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ Plan = &UnionPlan{}
)

// UnionPlan represents union plan.
type UnionPlan struct {
	log *xlog.Log

	// router
	router *router.Router

	// select ast
	node *sqlparser.Union

	// database
	database string

	// raw query
	RawQuery string

	// type
	typ PlanType

	Root PlanNode
}

// NewUnionPlan used to create SelectPlan.
func NewUnionPlan(log *xlog.Log, database string, query string, node *sqlparser.Union, router *router.Router) *UnionPlan {
	return &UnionPlan{
		log:      log,
		node:     node,
		router:   router,
		database: database,
		RawQuery: query,
		typ:      PlanTypeUnion,
	}
}

// Build used to build distributed querys.
func (p *UnionPlan) Build() error {
	var err error
	if p.Root, err = processUnion(p.log, p.router, p.database, p.node); err != nil {
		return err
	}

	p.Root.buildQuery(p.Root.getReferTables())
	return nil
}

// Type returns the type of the plan.
func (p *UnionPlan) Type() PlanType {
	return p.typ
}

// JSON returns the plan info.
func (p *UnionPlan) JSON() string {
	type limit struct {
		Offset int
		Limit  int
	}

	type explain struct {
		RawQuery    string                `json:",omitempty"`
		Project     string                `json:",omitempty"`
		Partitions  []xcontext.QueryTuple `json:",omitempty"`
		UnionType   *string               `json:",omitempty"`
		GatherMerge []string              `json:",omitempty"`
		Limit       *limit                `json:",omitempty"`
	}

	// Project.
	var prefix, project string
	tuples := p.Root.getFields()
	for _, tuple := range tuples {
		field := tuple.field
		if tuple.alias != "" {
			field = tuple.alias
		}
		project = fmt.Sprintf("%s%s%s", project, prefix, field)
		prefix = ", "
	}

	// Union.
	var uni *string
	if u, ok := p.Root.(*UnionNode); ok {
		uni = &u.Typ
	}

	var gatherMerge []string
	var lim *limit
	for _, sub := range p.Root.Children().Plans() {
		switch sub.Type() {
		case PlanTypeOrderby:
			plan := sub.(*OrderByPlan)
			for _, order := range plan.OrderBys {
				field := order.Field
				gatherMerge = append(gatherMerge, field)
			}
		case PlanTypeLimit:
			plan := sub.(*LimitPlan)
			lim = &limit{Offset: plan.Offset, Limit: plan.Limit}
		}
	}

	exp := &explain{Project: project,
		RawQuery:    p.RawQuery,
		Partitions:  p.Root.GetQuery(),
		UnionType:   uni,
		GatherMerge: gatherMerge,
		Limit:       lim,
	}
	bout, err := json.MarshalIndent(exp, "", "\t")
	if err != nil {
		return err.Error()
	}
	return common.BytesToString(bout)
}

// Size returns the memory size.
func (p *UnionPlan) Size() int {
	size := len(p.RawQuery)
	return size
}

// Children returns the children of the plan.
func (p *UnionPlan) Children() *PlanTree {
	return p.Root.Children()
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
		p := NewSelectPlan(log, database, "", part, router)
		if err := p.analyze(); err != nil {
			return nil, err
		}
		return p.Root, nil
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
