/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"encoding/json"
	"strings"

	"router"
	"xcontext"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ Plan = &SelectPlan{}
)

// SelectPlan represents select plan.
type SelectPlan struct {
	log *xlog.Log

	// router
	router *router.Router

	// select ast
	node *sqlparser.Select

	// database
	database string

	// raw query
	RawQuery string

	// type
	typ PlanType

	// mode
	ReqMode xcontext.RequestMode

	// children plans in select(such as: orderby, limit or join).
	children *PlanTree

	Root PlanNode
}

// NewSelectPlan used to create SelectPlan.
func NewSelectPlan(log *xlog.Log, database string, query string, node *sqlparser.Select, router *router.Router) *SelectPlan {
	return &SelectPlan{
		log:      log,
		node:     node,
		router:   router,
		database: database,
		RawQuery: query,
		typ:      PlanTypeSelect,
		children: NewPlanTree(),
	}
}

// analyze used to check the 'select' is at the support level, and get the db, table, etc..
// Unsupports:
// 1. subquery.
func (p *SelectPlan) analyze() error {
	// Check subquery.
	if hasSubquery(p.node) {
		return errors.New("unsupported: subqueries.in.select")
	}
	return nil
}

// Build used to build distributed querys.
// For now, we don't support subquery in select.
func (p *SelectPlan) Build() error {
	var err error
	log := p.log
	node := p.node

	// Check subquery.
	if err = p.analyze(); err != nil {
		return err
	}
	if p.Root, err = scanTableExprs(log, p.router, p.database, node.From); err != nil {
		return err
	}

	tbInfos := p.Root.getReferredTables()
	if node.Where != nil {
		joins, filters, err := parserWhereOrJoinExprs(node.Where.Expr, tbInfos)
		if err != nil {
			return err
		}
		if err = p.Root.pushFilter(filters); err != nil {
			return err
		}
		p.Root = p.Root.pushEqualCmpr(joins)
	}
	if p.Root, err = p.Root.calcRoute(); err != nil {
		return err
	}

	mn, ok := p.Root.(*MergeNode)
	if ok && mn.routeLen == 1 {
		node.From = mn.Sel.From
		node.Where = mn.Sel.Where
		if err = checkTbName(tbInfos, node); err != nil {
			return err
		}
		mn.Sel = node
		mn.buildQuery(tbInfos)
		return nil
	}

	p.Root.pushMisc(node)

	var groups []selectTuple
	fields, aggTyp, err := parserSelectExprs(node.SelectExprs, p.Root)
	if err != nil {
		return err
	}

	if groups, err = checkGroupBy(node.GroupBy, fields, p.router, tbInfos, ok); err != nil {
		return err
	}

	if groups, err = checkDistinct(node, groups, fields, p.router, tbInfos, ok); err != nil {
		return err
	}

	if err = p.Root.pushSelectExprs(fields, groups, node, aggTyp); err != nil {
		return err
	}

	if node.Having != nil {
		havings, err := parserHaving(node.Having.Expr, tbInfos)
		if err != nil {
			return err
		}
		if err = p.Root.pushHaving(havings); err != nil {
			return err
		}
	}

	if err = p.Root.pushOrderBy(node, fields); err != nil {
		return err
	}
	// Limit SubPlan.
	if node.Limit != nil {
		if err = p.Root.pushLimit(node); err != nil {
			return err
		}
	}

	p.Root.buildQuery(tbInfos)
	return nil
}

// Type returns the type of the plan.
func (p *SelectPlan) Type() PlanType {
	return p.typ
}

// JSON returns the plan info.
func (p *SelectPlan) JSON() string {
	type limit struct {
		Offset int
		Limit  int
	}

	type explain struct {
		RawQuery    string                `json:",omitempty"`
		Project     string                `json:",omitempty"`
		Partitions  []xcontext.QueryTuple `json:",omitempty"`
		Aggregate   []string              `json:",omitempty"`
		GatherMerge []string              `json:",omitempty"`
		HashGroupBy []string              `json:",omitempty"`
		Limit       *limit                `json:",omitempty"`
	}

	// Project.
	exprs := p.node.SelectExprs
	if m, ok := p.Root.(*MergeNode); ok {
		exprs = m.Sel.SelectExprs
	}
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", exprs)
	project := buf.String()

	// Aggregate.
	var aggregate []string
	var hashGroup []string
	var gatherMerge []string
	var lim *limit
	for _, sub := range p.Root.Children().Plans() {
		switch sub.Type() {
		case PlanTypeAggregate:
			plan := sub.(*AggregatePlan)
			for _, aggr := range plan.normalAggrs {
				aggregate = append(aggregate, aggr.Field)
			}
			for _, aggr := range plan.groupAggrs {
				hashGroup = append(hashGroup, aggr.Field)
			}
		case PlanTypeOrderby:
			plan := sub.(*OrderByPlan)
			for _, order := range plan.OrderBys {
				field := order.Field
				if order.Table != "" {
					field = strings.Join([]string{order.Table, order.Field}, ".")
				}
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
		Aggregate:   aggregate,
		GatherMerge: gatherMerge,
		HashGroupBy: hashGroup,
		Limit:       lim,
	}
	bout, err := json.MarshalIndent(exp, "", "\t")
	if err != nil {
		return err.Error()
	}
	return common.BytesToString(bout)
}

// Children returns the children of the plan.
func (p *SelectPlan) Children() *PlanTree {
	return p.children
}

// Size returns the memory size.
func (p *SelectPlan) Size() int {
	size := len(p.RawQuery)
	return size
}
