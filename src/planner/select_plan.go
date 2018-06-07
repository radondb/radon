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
	"fmt"
	"router"
	"xcontext"

	"github.com/pkg/errors"

	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/hack"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ Plan = &SelectPlan{}
)

// SelectPlan represents select plan
type SelectPlan struct {
	log *xlog.Log

	// router
	router *router.Router

	// insert ast
	node *sqlparser.Select

	// database
	database string

	// raw query
	RawQuery string

	// type
	typ PlanType

	// mode
	ReqMode xcontext.RequestMode

	// query and backend tuple
	Querys []xcontext.QueryTuple

	// children plans in select(such as: orderby, limit or join).
	children *PlanTree
}

// NewSelectPlan used to create SelectPlan
func NewSelectPlan(log *xlog.Log, database string, query string, node *sqlparser.Select, router *router.Router) *SelectPlan {
	return &SelectPlan{
		log:      log,
		node:     node,
		router:   router,
		database: database,
		RawQuery: query,
		typ:      PlanTypeSelect,
		Querys:   make([]xcontext.QueryTuple, 0, 16),
		children: NewPlanTree(),
	}
}

// analyze used to check the 'select' is at the support level, and get the db, table, etc..
// Unsupports:
// 1. subquery
func (p *SelectPlan) analyze() (string, string, string, error) {
	var shardDatabase string
	var shardTable string
	var aliasTable string
	var tableExpr *sqlparser.AliasedTableExpr
	node := p.node

	// Check subquery.
	if hasSubquery(node) || len(node.From) > 1 {
		return shardDatabase, shardTable, aliasTable, errors.New("unsupported: subqueries.in.select")
	}

	// Find the first table in the node.From.
	// Currently only support AliasedTableExpr, JoinTableExpr select.
	switch expr := (node.From[0]).(type) {
	case *sqlparser.AliasedTableExpr:
		tableExpr = expr
	case *sqlparser.JoinTableExpr:
		if v, ok := (expr.LeftExpr).(*sqlparser.AliasedTableExpr); ok {
			tableExpr = v
		}
	}

	if tableExpr != nil {
		aliasTable = tableExpr.As.String()

		switch expr := tableExpr.Expr.(type) {
		case sqlparser.TableName:
			if !expr.Qualifier.IsEmpty() {
				shardDatabase = expr.Qualifier.String()
			}
			shardTable = expr.Name.String()
		}
	}
	return shardDatabase, shardTable, aliasTable, nil
}

// Build used to build distributed querys.
// For now, we don't support subquery in select.
func (p *SelectPlan) Build() error {
	var err error
	var shardTable string
	var aliasTable string
	var shardDatabase string

	log := p.log
	node := p.node
	if shardDatabase, shardTable, aliasTable, err = p.analyze(); err != nil {
		return err
	}
	if shardDatabase == "" {
		shardDatabase = p.database
	}
	if aliasTable == "" {
		aliasTable = shardTable
	}

	// Get the routing segments info.
	shardkey, err := p.router.ShardKey(shardDatabase, shardTable)
	if err != nil {
		return err
	}
	segments, err := getDMLRouting(shardDatabase, shardTable, shardkey, node.Where, p.router)
	if err != nil {
		return err
	}

	// Add sub-plans.
	children := p.children
	if len(segments) > 1 {
		tuples, err := parserSelectExprs(node.SelectExprs)
		if err != nil {
			return err
		}
		// Distinct SubPlan.
		distinctPlan := NewDistinctPlan(log, node)
		if err := distinctPlan.Build(); err != nil {
			return err
		}
		children.Add(distinctPlan)

		// Join SubPlan.
		joinPlan := NewJoinPlan(log, node)
		if err := joinPlan.Build(); err != nil {
			return err
		}
		children.Add(joinPlan)

		// Aggregate SubPlan.
		aggrPlan := NewAggregatePlan(log, node, tuples)
		if err := aggrPlan.Build(); err != nil {
			return err
		}
		children.Add(aggrPlan)
		node.SelectExprs = aggrPlan.ReWritten()

		// Orderby SubPlan.
		orderPlan := NewOrderByPlan(log, node, tuples)
		if err := orderPlan.Build(); err != nil {
			return err
		}
		children.Add(orderPlan)

		// Limit SubPlan.
		if node.Limit != nil {
			limitPlan := NewLimitPlan(log, node)
			if err := limitPlan.Build(); err != nil {
				return err
			}
			children.Add(limitPlan)
			// Rewrite the limit clause.
			node.Limit = limitPlan.ReWritten()
		}
	}

	// Rewritten the query.
	for _, segment := range segments {
		tn := sqlparser.TableName{
			Name:      sqlparser.NewTableIdent(segment.Table),
			Qualifier: sqlparser.NewTableIdent(shardDatabase),
		}
		as := fmt.Sprintf(" as %s", aliasTable)
		buf := sqlparser.NewTrackedBuffer(nil)
		buf.Myprintf("select %v%s%v from %v%s%v%v%v%v%v",
			node.Comments, node.Hints, node.SelectExprs,
			tn, as,
			node.Where,
			node.GroupBy, node.Having, node.OrderBy,
			node.Limit)
		rewritten := buf.String()

		tuple := xcontext.QueryTuple{
			Query:   rewritten,
			Backend: segment.Backend,
			Range:   segment.Range.String(),
		}
		p.Querys = append(p.Querys, tuple)
	}
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
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf("%v", p.node.SelectExprs)
	project := buf.String()

	// Aggregate.
	var aggregate []string
	var hashGroup []string
	var gatherMerge []string
	var lim *limit
	for _, sub := range p.children.Plans() {
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
				gatherMerge = append(gatherMerge, order.Field)
			}
		case PlanTypeLimit:
			plan := sub.(*LimitPlan)
			lim = &limit{Offset: plan.Offset, Limit: plan.Limit}
		}
	}

	exp := &explain{Project: project,
		RawQuery:    p.RawQuery,
		Partitions:  p.Querys,
		Aggregate:   aggregate,
		GatherMerge: gatherMerge,
		HashGroupBy: hashGroup,
		Limit:       lim,
	}
	bout, err := json.MarshalIndent(exp, "", "\t")
	if err != nil {
		return err.Error()
	}
	return hack.String(bout)
}

// Children returns the children of the plan.
func (p *SelectPlan) Children() *PlanTree {
	return p.children
}

// Size returns the memory size.
func (p *SelectPlan) Size() int {
	size := len(p.RawQuery)
	for _, q := range p.Querys {
		size += len(q.Query)
	}
	return size
}
