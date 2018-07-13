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

	"router"
	"xcontext"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/hack"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ Plan = &DeletePlan{}
)

// DeletePlan represents delete plan
type DeletePlan struct {
	log *xlog.Log

	// router
	router *router.Router

	// delete ast
	node *sqlparser.Delete

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
}

// NewDeletePlan used to create DeletePlan
func NewDeletePlan(log *xlog.Log, database string, query string, node *sqlparser.Delete, router *router.Router) *DeletePlan {
	return &DeletePlan{
		log:      log,
		node:     node,
		router:   router,
		database: database,
		RawQuery: query,
		typ:      PlanTypeDelete,
		Querys:   make([]xcontext.QueryTuple, 0, 16),
	}
}

// analyze used to analyze the 'delete' is at the support level.
func (p *DeletePlan) analyze() error {
	node := p.node
	// analyze subquery.
	if hasSubquery(node) {
		return errors.New("unsupported: subqueries.in.delete")
	}
	if node.Where == nil {
		return errors.New("unsupported: missing.where.clause.in.DML")
	}
	return nil
}

// Build used to build distributed querys.
func (p *DeletePlan) Build() error {
	if err := p.analyze(); err != nil {
		return err
	}

	node := p.node
	// Database.
	database := p.database
	if !node.Table.Qualifier.IsEmpty() {
		database = node.Table.Qualifier.String()
	}
	table := node.Table.Name.String()

	// Sharding key.
	shardkey, err := p.router.ShardKey(database, table)
	if err != nil {
		return err
	}

	// Get the routing segments info.
	segments, err := getDMLRouting(database, table, shardkey, node.Where, p.router)
	if err != nil {
		return err
	}

	// Rewritten the query.
	for _, segment := range segments {
		buf := sqlparser.NewTrackedBuffer(nil)
		buf.Myprintf("delete %vfrom %s.%s%v%v%v", node.Comments, database, segment.Table, node.Where, node.OrderBy, node.Limit)
		tuple := xcontext.QueryTuple{
			Query:   buf.String(),
			Backend: segment.Backend,
			Range:   segment.Range.String(),
		}
		p.Querys = append(p.Querys, tuple)
	}
	return nil
}

// Type returns the type of the plan.
func (p *DeletePlan) Type() PlanType {
	return p.typ
}

// JSON returns the plan info.
func (p *DeletePlan) JSON() string {
	type explain struct {
		RawQuery   string                `json:",omitempty"`
		Partitions []xcontext.QueryTuple `json:",omitempty"`
	}

	// Partitions.
	var parts []xcontext.QueryTuple
	parts = append(parts, p.Querys...)
	exp := &explain{
		RawQuery:   p.RawQuery,
		Partitions: parts,
	}
	bout, err := json.MarshalIndent(exp, "", "\t")
	if err != nil {
		return err.Error()
	}
	return hack.String(bout)
}

// Children returns the children of the plan.
func (p *DeletePlan) Children() *PlanTree {
	return nil
}

// Size returns the memory size.
func (p *DeletePlan) Size() int {
	size := len(p.RawQuery)
	for _, q := range p.Querys {
		size += len(q.Query)
	}
	return size
}
