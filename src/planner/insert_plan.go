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
	"sort"

	"router"
	"xcontext"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/hack"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ Plan = &InsertPlan{}
)

// InsertPlan represents insertion plan
type InsertPlan struct {
	log *xlog.Log

	// router
	router *router.Router

	// insert ast
	node *sqlparser.Insert

	// database
	database string

	// raw query
	RawQuery string

	// type
	Typ PlanType

	// mode
	ReqMode xcontext.RequestMode

	// query and backend tuple
	Querys []xcontext.QueryTuple
}

// NewInsertPlan used to create InsertPlan
func NewInsertPlan(log *xlog.Log, database string, query string, node *sqlparser.Insert, router *router.Router) *InsertPlan {
	return &InsertPlan{
		log:      log,
		node:     node,
		router:   router,
		database: database,
		RawQuery: query,
		Typ:      PlanTypeInsert,
		Querys:   make([]xcontext.QueryTuple, 0, 16),
	}
}

// Build used to build distributed querys.
func (p *InsertPlan) Build() error {
	node := p.node

	database := p.database
	// Qualifier is database in the insert query, such as "db.t1".
	if !node.Table.Qualifier.IsEmpty() {
		database = node.Table.Qualifier.String()
	}
	table := node.Table.Name.String()

	// Get the shard key.
	shardKey, err := p.router.ShardKey(database, table)
	if err != nil {
		return err
	}

	// Check the OnDup.
	if len(node.OnDup) > 0 {
		// analyze shardkey changing.
		if isShardKeyChanging(sqlparser.UpdateExprs(node.OnDup), shardKey) {
			return errors.New("unsupported: cannot.update.shard.key")
		}
	}

	// Find the shard key index.
	idx := -1
	for i, column := range node.Columns {
		if column.String() == shardKey {
			idx = i
			break
		}
	}
	if idx == -1 {
		return errors.Errorf("unsupported: shardkey.column[%v].missing", shardKey)
	}

	// Rebuild distributed querys.
	type valTuple struct {
		backend string
		table   string
		rangi   string
		vals    sqlparser.Values
	}
	vals := make(map[string]*valTuple)
	rows, ok := node.Rows.(sqlparser.Values)
	if !ok {
		return errors.Errorf("unsupported: rows.can.not.be.subquery[%T]", node.Rows)
	}

	for _, row := range rows {
		if idx >= len(row) {
			return errors.Errorf("unsupported: shardkey[%v].out.of.index:[%v]", shardKey, idx)
		}
		shardVal, ok := row[idx].(*sqlparser.SQLVal)
		if !ok {
			return errors.Errorf("unsupported: shardkey[%v].type.canot.be[%T]", shardKey, row[idx])
		}

		segments, err := p.router.Lookup(database, table, shardVal, shardVal)
		if err != nil {
			return err
		}
		rewrittenTable := segments[0].Table
		backend := segments[0].Backend
		rangi := segments[0].Range.String()
		val, ok := vals[rewrittenTable]
		if !ok {
			val = &valTuple{
				backend: backend,
				table:   rewrittenTable,
				rangi:   rangi,
				vals:    make(sqlparser.Values, 0, 16),
			}
			vals[rewrittenTable] = val
		}
		val.vals = append(val.vals, row)
	}

	// Rebuild querys with router info.
	for rewritten, v := range vals {
		buf := sqlparser.NewTrackedBuffer(nil)
		buf.Myprintf("%s %v%sinto %s.%s%v %v%v", node.Action, node.Comments, node.Ignore, database, rewritten, node.Columns, v.vals, node.OnDup)
		tuple := xcontext.QueryTuple{
			Query:   buf.String(),
			Backend: v.backend,
			Range:   v.rangi,
		}
		p.Querys = append(p.Querys, tuple)
	}
	return nil
}

// Type returns the type of the plan.
func (p *InsertPlan) Type() PlanType {
	return p.Typ
}

// JSON returns the plan info.
func (p *InsertPlan) JSON() string {
	type explain struct {
		RawQuery   string                `json:",omitempty"`
		Partitions []xcontext.QueryTuple `json:",omitempty"`
	}

	var parts []xcontext.QueryTuple
	// Sort.
	sort.Sort(xcontext.QueryTuples(p.Querys))
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
func (p *InsertPlan) Children() *PlanTree {
	return nil
}

// Size returns the memory size.
func (p *InsertPlan) Size() int {
	size := len(p.RawQuery)
	for _, q := range p.Querys {
		size += len(q.Query)
	}
	return size
}
