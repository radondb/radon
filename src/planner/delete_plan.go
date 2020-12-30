/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"planner/builder"
	"router"
	"xcontext"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
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
	// Currently we both support parse delete with single or multi tables, but only support build plans for single table.
	if !node.IsSingleTable {
		return errors.New("unsupported: currently.not.support.multitables.in.delete")
	}
	// Currently not support deal with partitions.
	if node.Partitions != nil {
		return errors.New("unsupported: currently.not.support.partitions.in.delete")
	}
	// Not support subquery.
	if hasSubquery(node) {
		return errors.New("unsupported: subqueries.in.delete")
	}
	return nil
}

// Build used to build distributed querys.
func (p *DeletePlan) Build() error {
	if err := p.analyze(); err != nil {
		return err
	}

	newNode := *p.node
	// For single table, the len(TableRefs)=1 and the type of TableExpr must be AliasedTableExpr.
	newAliseExpr := newNode.TableRefs[0].(*sqlparser.AliasedTableExpr)
	tableID := newAliseExpr.Expr.(sqlparser.TableName).Name
	databaseID := newAliseExpr.Expr.(sqlparser.TableName).Qualifier
	if databaseID.IsEmpty() {
		databaseID = sqlparser.NewTableIdent(p.database)
		// Construction a new sqlparser.SimpleTableExpr
		newAliseExpr.Expr = sqlparser.TableName{Name: tableID, Qualifier: databaseID}
	}

	var segments []router.Segment
	var err error
	if newNode.Where == nil {
		// delete all datas, send sql to different backends, except for single table which has only one backend.
		segments, err = p.router.Lookup(databaseID.String(), tableID.String(), nil, nil)
		if err != nil {
			return err
		}
	} else {
		// Sharding key.
		shardkey, err := p.router.ShardKey(databaseID.String(), tableID.String())
		if err != nil {
			return err
		}

		// Get the routing segments info.
		segments, err = builder.LookupFromWhere(databaseID.String(), tableID.String(), shardkey, newNode.Where, p.router)
		if err != nil {
			return err
		}
	}

	// Rewritten the newNode to produce a new query.
	for _, segment := range segments {
		tableID = sqlparser.NewTableIdent(segment.Table)
		newAliseExpr.Expr = sqlparser.TableName{Name: tableID, Qualifier: databaseID}
		tuple := xcontext.QueryTuple{
			Query:   sqlparser.String(&newNode),
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
	out, err := common.ToJSONString(exp, false, "", "\t")
	if err != nil {
		return err.Error()
	}
	return out
}

// Size returns the memory size.
func (p *DeletePlan) Size() int {
	size := len(p.RawQuery)
	for _, q := range p.Querys {
		size += len(q.Query)
	}
	return size
}
