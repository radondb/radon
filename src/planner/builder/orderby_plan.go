/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

import (
	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ ChildPlan = &OrderByPlan{}
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
	log  *xlog.Log
	node sqlparser.OrderBy
	root PlanNode
	// The indexes mark the fields to be removed.
	RemovedIdxs []int
	OrderBys    []OrderBy `json:"OrderBy(s)"`
	typ         ChildType
}

// NewOrderByPlan used to create OrderByPlan.
func NewOrderByPlan(log *xlog.Log, node sqlparser.OrderBy, root PlanNode) *OrderByPlan {
	return &OrderByPlan{
		log:  log,
		node: node,
		root: root,
		typ:  ChildTypeOrderby,
	}
}

// analyze used to check the 'order by' is at the support level.
func (p *OrderByPlan) analyze() error {
	tbInfos := p.root.getReferTables()
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
				if _, ok := p.root.(*UnionNode); ok {
					return errors.Errorf("unsupported: table.'%s'.from.one.of.the.SELECTs.cannot.be.used.in.field.list", orderBy.Table)
				}
				if _, ok := tbInfos[orderBy.Table]; !ok {
					return errors.Errorf("unsupported: unknow.table.in.order.by.field[%s.%s]", orderBy.Table, orderBy.Field)
				}
			}

			ok, tuple := checkInTuple(orderBy.Field, orderBy.Table, p.root.getFields())
			if !ok {
				if _, ok := p.root.(*UnionNode); ok {
					return errors.Errorf("unsupported: unknown.column.'%s'.in.'order.clause'", orderBy.Field)
				}

				tablename := orderBy.Table
				if tablename == "" {
					if len(tbInfos) == 1 {
						tablename, _ = getOneTableInfo(tbInfos)
					} else {
						return errors.Errorf("unsupported: column.'%s'.in.order.clause.is.ambiguous", orderBy.Field)
					}
				}
				// If `orderby.field` not exists in the field list,
				// we need push it into field list and record in RemovedIdxs.
				tuple = &selectTuple{
					expr:        &sqlparser.AliasedExpr{Expr: e},
					field:       orderBy.Field,
					referTables: []string{tablename},
					isCol:       true,
				}
				index, _ := p.root.pushSelectExpr(*tuple)
				p.RemovedIdxs = append(p.RemovedIdxs, index)
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
func (p *OrderByPlan) Type() ChildType {
	return p.typ
}

// JSON returns the plan info.
func (p *OrderByPlan) JSON() string {
	out, err := common.ToJSONString(p, false, "", "\t")
	if err != nil {
		return err.Error()
	}
	return out
}
