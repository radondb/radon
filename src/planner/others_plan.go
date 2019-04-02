/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"encoding/json"
	"sort"

	"router"
	"xcontext"

	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ Plan = &OthersPlan{}
)

// OthersPlan -- represents a special plan.
type OthersPlan struct {
	log *xlog.Log

	// router
	router *router.Router

	// ast
	node sqlparser.Statement

	// database
	database string

	// type
	typ PlanType

	// raw query
	RawQuery string

	// mode
	ReqMode xcontext.RequestMode

	// query and backend tuple
	Querys []xcontext.QueryTuple
}

// NewOthersPlan -- used to create OthersPlan.
func NewOthersPlan(log *xlog.Log, database string, query string, node sqlparser.Statement, router *router.Router) *OthersPlan {
	return &OthersPlan{
		log:      log,
		node:     node,
		router:   router,
		database: database,
		typ:      PlanTypeOthers,
		RawQuery: query,
		Querys:   make([]xcontext.QueryTuple, 0, 16),
	}
}

// Build used to build distributed querys.
func (p *OthersPlan) Build() error {
	node := p.node
	router := p.router

	switch node := node.(type) {
	// Checksum Table.
	case *sqlparser.Checksum:
		database := p.database
		if !node.Table.Qualifier.IsEmpty() {
			database = node.Table.Qualifier.String()
		}
		table := node.Table.Name.String()
		route, err := router.TableConfig(database, table)
		if err != nil {
			return err
		}

		// Global table.
		if route.ShardKey == "" {
			segment := route.Partitions[0]
			tuple := xcontext.QueryTuple{
				Query:   p.RawQuery,
				Backend: segment.Backend,
				Range:   segment.Segment,
			}
			p.Querys = append(p.Querys, tuple)
		} else {
			segments := route.Partitions
			for _, segment := range segments {
				buf := sqlparser.NewTrackedBuffer(nil)
				buf.Myprintf("checksum table %s.%s", database, segment.Table)
				tuple := xcontext.QueryTuple{
					Query:   buf.String(),
					Backend: segment.Backend,
					Range:   segment.Segment,
				}
				p.Querys = append(p.Querys, tuple)
			}
		}
	}
	return nil
}

// Type returns the type of the plan.
func (p *OthersPlan) Type() PlanType {
	return p.typ
}

// JSON returns the plan info.
func (p *OthersPlan) JSON() string {
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
	return common.BytesToString(bout)
}

// Children returns the children of the plan.
func (p *OthersPlan) Children() *PlanTree {
	return nil
}

// Size returns the memory size.
func (p *OthersPlan) Size() int {
	return 0
}
