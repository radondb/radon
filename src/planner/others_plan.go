/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
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
	switch node := p.node.(type) {
	// Checksum Table.
	case *sqlparser.Checksum:
		newNode := *node
		// We`ll rewrite ast on newNode and the table`s format should be like "db.t1", so the "Qualifier" in ast should not be empty.
		if newNode.Tables[0].Qualifier.IsEmpty() {
			newNode.Tables[0].Qualifier = sqlparser.NewTableIdent(p.database)
		}
		database := newNode.Tables[0].Qualifier.String()
		table := newNode.Tables[0].Name.String()
		route, err := p.router.TableConfig(database, table)
		if err != nil {
			return err
		}

		methodType, err := p.router.PartitionType(database, table)
		if err != nil {
			return err
		}
		switch methodType {
		case router.MethodTypeGlobal, router.MethodTypeSingle:
			segment := route.Partitions[0]
			tuple := xcontext.QueryTuple{
				Query:   sqlparser.String(&newNode),
				Backend: segment.Backend,
				Range:   segment.Segment,
			}
			p.Querys = append(p.Querys, tuple)
		case router.MethodTypeHash, router.MethodTypeList:
			segments := route.Partitions
			for _, segment := range segments {
				newNode.Tables[0].Name = sqlparser.NewTableIdent(segment.Table)
				tuple := xcontext.QueryTuple{
					Query:   sqlparser.String(&newNode),
					Backend: segment.Backend,
					Range:   segment.Segment,
				}
				p.Querys = append(p.Querys, tuple)
			}
		default:
			return errors.Errorf("unsupported: radon.not.support.method.type[%s].", methodType)
		}
	case *sqlparser.Optimize:
		newNode := *node
		// We`ll rewrite ast on newNode and the table`s format should be like "db.t1", so the "Qualifier" in ast should not be empty.
		if newNode.Tables[0].Qualifier.IsEmpty() {
			newNode.Tables[0].Qualifier = sqlparser.NewTableIdent(p.database)
		}
		database := newNode.Tables[0].Qualifier.String()
		table := newNode.Tables[0].Name.String()

		route, err := p.router.TableConfig(database, table)
		if err != nil {
			return err
		}
		for _, segment := range route.Partitions {
			newNode.Tables[0].Name = sqlparser.NewTableIdent(segment.Table)
			tuple := xcontext.QueryTuple{
				Query:   sqlparser.String(&newNode),
				Backend: segment.Backend,
				Range:   segment.Segment,
			}
			p.Querys = append(p.Querys, tuple)
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
	out, err := common.ToJSONString(exp, false, "", "\t")
	if err != nil {
		return err.Error()
	}
	return out
}

// Size returns the memory size.
func (p *OthersPlan) Size() int {
	return 0
}
