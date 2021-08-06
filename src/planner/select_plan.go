/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"backend"
	"errors"
	"strings"

	"planner/builder"
	"router"
	"xcontext"

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

	scatter *backend.Scatter

	// select ast
	node *sqlparser.Select

	// database
	database string

	// raw query
	RawQuery string

	// type
	typ PlanType

	Root builder.PlanNode
}

// NewSelectPlan used to create SelectPlan.
func NewSelectPlan(log *xlog.Log, database string, query string, node *sqlparser.Select, router *router.Router, scatter *backend.Scatter) *SelectPlan {
	return &SelectPlan{
		log:      log,
		node:     node,
		router:   router,
		scatter:  scatter,
		database: database,
		RawQuery: query,
		typ:      PlanTypeSelect,
	}
}

// Build used to build distributed querys.
// For now, we don't support subquery in select.
func (p *SelectPlan) Build() error {
	var err error
	// Check subquery.
	if hasSubquery(p.node) {
		return errors.New("unsupported: subqueries.in.select")
	}
	p.Root, err = builder.BuildNode(p.log, p.router, p.scatter, p.database, p.node)
	return err
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

	type join struct {
		Type     string
		Strategy string
	}

	type explain struct {
		RawQuery    string                `json:",omitempty"`
		Project     string                `json:",omitempty"`
		Partitions  []xcontext.QueryTuple `json:",omitempty"`
		Join        *join                 `json:",omitempty"`
		Aggregate   []string              `json:",omitempty"`
		GatherMerge []string              `json:",omitempty"`
		HashGroupBy []string              `json:",omitempty"`
		Limit       *limit                `json:",omitempty"`
	}

	var joins *join
	if j, ok := p.Root.(*builder.JoinNode); ok {
		joins = &join{}
		switch j.Strategy {
		case builder.Cartesian:
			joins.Strategy = "Cartesian Join"
		case builder.SortMerge:
			joins.Strategy = "Sort Merge Join"
		case builder.NestLoop:
			joins.Strategy = "Nested Loop Join"
		}
		if j.IsLeftJoin {
			joins.Type = "LEFT JOIN"
		} else {
			if j.Strategy == builder.Cartesian {
				joins.Type = "CROSS JOIN"
			} else {
				joins.Type = "INNER JOIN"
			}
		}
	}

	// Aggregate.
	var aggregate []string
	var hashGroup []string
	var gatherMerge []string
	var lim *limit
	for _, sub := range p.Root.Children() {
		switch sub.Type() {
		case builder.ChildTypeAggregate:
			plan := sub.(*builder.AggregatePlan)
			for _, aggr := range plan.NormalAggregators() {
				aggregate = append(aggregate, aggr.Field)
			}
			for _, aggr := range plan.GroupAggregators() {
				hashGroup = append(hashGroup, aggr.Field)
			}
		case builder.ChildTypeOrderby:
			plan := sub.(*builder.OrderByPlan)
			for _, order := range plan.OrderBys {
				field := order.Field
				if order.Table != "" {
					field = strings.Join([]string{order.Table, order.Field}, ".")
				}
				gatherMerge = append(gatherMerge, field)
			}
		case builder.ChildTypeLimit:
			plan := sub.(*builder.LimitPlan)
			lim = &limit{Offset: plan.Offset, Limit: plan.Limit}
		}
	}

	exp := &explain{Project: builder.GetProject(p.Root),
		RawQuery:    p.RawQuery,
		Partitions:  p.Root.GetQuery(),
		Join:        joins,
		Aggregate:   aggregate,
		GatherMerge: gatherMerge,
		HashGroupBy: hashGroup,
		Limit:       lim,
	}
	out, err := common.ToJSONString(exp, false, "", "\t")
	if err != nil {
		return err.Error()
	}
	return out
}

// Size returns the memory size.
func (p *SelectPlan) Size() int {
	size := len(p.RawQuery)
	return size
}
