/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"sort"

	"router"
	"xcontext"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
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
	newNode := *(p.node)

	// 1. Currently insert/replace not support subquery.
	rows, ok := newNode.Rows.(sqlparser.Values)
	if !ok {
		return errors.Errorf("unsupported: rows.can.not.be.subquery[%T]", newNode.Rows)
	}

	// 2. Currently insert/replace not support partitions.
	if len(newNode.Partitions) != 0 {
		return errors.Errorf("unsupported: radon.now.not.support.insert.with.partition.")
	}

	// We`ll rewrite ast on newNode and the table`s format should be like "db.t1", so the "Qualifier" in ast should be not empty.
	if newNode.Table.Qualifier.IsEmpty() {
		newNode.Table.Qualifier = sqlparser.NewTableIdent(p.database)
	}
	database := newNode.Table.Qualifier.String()
	table := newNode.Table.Name.String()

	methodType, err := p.router.PartitionType(database, table)
	if err != nil {
		return err
	}
	switch methodType {
	case router.MethodTypeGlobal, router.MethodTypeSingle:
		segments, err := p.router.Lookup(database, table, nil, nil)
		if err != nil {
			return err
		}

		for _, segment := range segments {
			tuple := xcontext.QueryTuple{
				Query:   sqlparser.String(&newNode),
				Backend: segment.Backend,
				Range:   segment.Range.String(),
			}
			p.Querys = append(p.Querys, tuple)
		}
		return nil
	case router.MethodTypeHash, router.MethodTypeList:
		// Get the shard key.
		shardKey, err := p.router.ShardKey(database, table)
		if err != nil {
			return err
		}

		// Check the OnDup.
		if len(newNode.OnDup) > 0 {
			// analyze whether update shardkey.
			if isUpdateShardKey(sqlparser.UpdateExprs(newNode.OnDup), shardKey) {
				return errors.New("unsupported: cannot.update.shard.key")
			}
		}

		// Find the shard key index.
		idx := -1
		for i, column := range newNode.Columns {
			if column.EqualString(shardKey) {
				idx = i
				break
			}
		}
		if idx == -1 {
			return errors.Errorf("unsupported: shardkey.column[%v].missing", shardKey)
		}

		// Rebuild distributed querys.
		type rowsTuple struct {
			backend string
			table   string
			rangi   string
			rows    sqlparser.Values
		}

		// key: partition table, value: rowsTuple.
		rTuples := make(map[string]*rowsTuple)

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
			partTable := segments[0].Table
			backend := segments[0].Backend
			rangi := segments[0].Range.String()
			rTuple, ok := rTuples[partTable]
			if !ok {
				rTuple = &rowsTuple{
					backend: backend,
					table:   partTable,
					rangi:   rangi,
					rows:    make(sqlparser.Values, 0, 16),
				}
				rTuples[partTable] = rTuple
			}
			rTuple.rows = append(rTuple.rows, row)
		}

		// sorts SQL by partitionTable in increasing order to avoid deadlock #605.
		partTables := []string{}
		for partTable, _ := range rTuples {
			partTables = append(partTables, partTable)
		}
		sort.Strings(partTables)

		// Rebuild querys with router info.
		for _, partTable := range partTables {
			v := rTuples[partTable]
			newNode.Table.Name = sqlparser.NewTableIdent(partTable)
			newNode.Rows = v.rows
			tuple := xcontext.QueryTuple{
				Query:   sqlparser.String(&newNode),
				Backend: v.backend,
				Range:   v.rangi,
			}
			p.Querys = append(p.Querys, tuple)
		}
		return nil
	default:
		return errors.Errorf("unsupported: radon.not.support.method.type[%s].", methodType)
	}
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
func (p *InsertPlan) Size() int {
	size := len(p.RawQuery)
	for _, q := range p.Querys {
		size += len(q.Query)
	}
	return size
}
