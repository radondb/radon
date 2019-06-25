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

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ Plan = &OrderByPlan{}
)

// Direction type.
type Direction string

const (
	// ASC enum.
	ASC Direction = "ASC"

	// DESC enum.
	DESC Direction = "DESC"
)

// OrderBy tuple.
type OrderBy struct {
	Field     string
	Table     string
	Direction Direction
}

// OrderByPlan represents order-by plan.
type OrderByPlan struct {
	log      *xlog.Log
	node     sqlparser.OrderBy
	tuples   []selectTuple
	tbInfos  map[string]*TableInfo
	OrderBys []OrderBy `json:"OrderBy(s)"`
	typ      PlanType
}

// NewOrderByPlan used to create OrderByPlan.
func NewOrderByPlan(log *xlog.Log, node sqlparser.OrderBy, tuples []selectTuple, tbInfos map[string]*TableInfo) *OrderByPlan {
	return &OrderByPlan{
		log:     log,
		node:    node,
		tuples:  tuples,
		tbInfos: tbInfos,
		typ:     PlanTypeOrderby,
	}
}

// analyze used to check the 'order by' is at the support level.
// Supports:
// 1. sqlparser.ColName: 'select a from t order by a'
//
// Unsupported(orderby field must be in select list):
// 1. 'select a from t order by b'
func (p *OrderByPlan) analyze() error {
	for _, o := range p.node {
		switch e := o.Expr.(type) {
		case *sqlparser.ColName:
			orderBy := OrderBy{}
			switch o.Direction {
			case "desc":
				orderBy.Direction = DESC
			case "asc":
				orderBy.Direction = ASC
			}
			orderBy.Field = e.Name.String()
			orderBy.Table = e.Qualifier.Name.String()
			if orderBy.Table != "" {
				if _, ok := p.tbInfos[orderBy.Table]; !ok {
					return errors.Errorf("unsupported: unknow.table.in.order.by.field[%s.%s]", orderBy.Table, orderBy.Field)
				}
			}

			ok, tuple := checkInTuple(orderBy.Field, orderBy.Table, p.tuples)
			if !ok {
				field := orderBy.Field
				if orderBy.Table != "" {
					field = fmt.Sprintf("%s.%s", orderBy.Table, orderBy.Field)
				}
				return errors.Errorf("unsupported: orderby[%s].should.in.select.list", field)
			}

			if tuple.field != "*" {
				if tuple.alias != "" {
					orderBy.Field = tuple.alias
				} else {
					orderBy.Field = tuple.field
				}
			}
			p.OrderBys = append(p.OrderBys, orderBy)
		default:
			buf := sqlparser.NewTrackedBuffer(nil)
			e.Format(buf)
			return errors.Errorf("unsupported: orderby:[%+v].type.should.be.colname", buf.String())
		}
	}
	return nil
}

// Build used to build distributed querys.
func (p *OrderByPlan) Build() error {
	return p.analyze()
}

// Type returns the type of the plan.
func (p *OrderByPlan) Type() PlanType {
	return p.typ
}

// JSON returns the plan info.
func (p *OrderByPlan) JSON() string {
	bout, err := json.MarshalIndent(p, "", "\t")
	if err != nil {
		return err.Error()
	}
	return string(bout)
}

// Children returns the children of the plan.
func (p *OrderByPlan) Children() *PlanTree {
	return nil
}

// Size returns the memory size.
func (p *OrderByPlan) Size() int {
	return 0
}
