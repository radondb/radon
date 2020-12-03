/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"planner/builder"
	"router"
	"xcontext"

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

	Root builder.PlanNode
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
	p.Root, err = builder.BuildNode(p.log, p.router, p.database, p.node)
	return err
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

	// Union.
	var uni *string
	if u, ok := p.Root.(*builder.UnionNode); ok {
		uni = &u.Typ
	}

	var gatherMerge []string
	var lim *limit
	for _, sub := range p.Root.Children() {
		switch sub.Type() {
		case builder.ChildTypeOrderby:
			plan := sub.(*builder.OrderByPlan)
			for _, order := range plan.OrderBys {
				field := order.Field
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
		UnionType:   uni,
		GatherMerge: gatherMerge,
		Limit:       lim,
	}
	out, err := common.ToJSONString(exp, false, "", "\t")
	if err != nil {
		return err.Error()
	}
	return out
}

// Size returns the memory size.
func (p *UnionPlan) Size() int {
	size := len(p.RawQuery)
	return size
}
