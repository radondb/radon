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
	_ Plan = &UpdatePlan{}
)

// UpdatePlan represents delete plan
type UpdatePlan struct {
	log *xlog.Log

	// router
	router *router.Router

	// update ast
	node *sqlparser.Update

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

// NewUpdatePlan used to create UpdatePlan
func NewUpdatePlan(log *xlog.Log, database string, query string, node *sqlparser.Update, router *router.Router) *UpdatePlan {
	return &UpdatePlan{
		log:      log,
		node:     node,
		router:   router,
		database: database,
		RawQuery: query,
		typ:      PlanTypeUpdate,
		Querys:   make([]xcontext.QueryTuple, 0, 16),
	}
}

// analyze used to analyze the 'update' is at the support level.
func (p *UpdatePlan) analyze() error {
	node := p.node
	// analyze subquery.
	if hasSubquery(p.node) {
		return errors.New("unsupported: subqueries.in.update")
	}
	if node.Where == nil {
		return errors.New("unsupported: missing.where.clause.in.DML")
	}
	return nil
}

// Build used to build distributed querys.
func (p *UpdatePlan) Build() error {
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

	// analyze shardkey changing.
	if isShardKeyChanging(node.Exprs, shardkey) {
		return errors.New("unsupported: cannot.update.shard.key")
	}

	// Get the routing segments info.
	segments, err := getDMLRouting(database, table, shardkey, node.Where, p.router)
	if err != nil {
		return err
	}

	// Rewrite the query.
	for _, segment := range segments {
		buf := sqlparser.NewTrackedBuffer(nil)
		buf.Myprintf("update %v%s.%s set %v%v%v%v", node.Comments, database, segment.Table, node.Exprs, node.Where, node.OrderBy, node.Limit)
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
func (p *UpdatePlan) Type() PlanType {
	return p.typ
}

// JSON returns the plan info.
func (p *UpdatePlan) JSON() string {
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
func (p *UpdatePlan) Children() *PlanTree {
	return nil
}

// Size returns the memory size.
func (p *UpdatePlan) Size() int {
	size := len(p.RawQuery)
	for _, q := range p.Querys {
		size += len(q.Query)
	}
	return size
}
