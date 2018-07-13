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
	"strconv"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/hack"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ Plan = &LimitPlan{}
)

// LimitPlan represents order-by plan.
type LimitPlan struct {
	log *xlog.Log

	node      *sqlparser.Select
	rewritten *sqlparser.Limit
	Offset    int
	Limit     int

	// type
	typ PlanType
}

// NewLimitPlan used to create LimitPlan.
func NewLimitPlan(log *xlog.Log, node *sqlparser.Select) *LimitPlan {
	return &LimitPlan{
		log:  log,
		node: node,
		typ:  PlanTypeLimit,
	}
}

// analyze used to analyze the 'order by' is at the support level.
func (p *LimitPlan) analyze() error {
	node := p.node.Limit
	if node == nil {
		return nil
	}

	ok := true
	sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node.(type) {
		// Limit clause must be SQLVal type.
		case *sqlparser.Limit:
			return true, nil
		case *sqlparser.SQLVal:
			val := node.(*sqlparser.SQLVal)
			if val.Type != sqlparser.IntVal {
				ok = false
				return false, nil
			}
			return true, nil
		default:
			ok = false
			return false, nil
		}
	}, node)

	if !ok {
		return errors.New("unsupported: limit.offset.or.counts.must.be.IntVal")
	}
	return nil
}

// Build used to build distributed querys.
func (p *LimitPlan) Build() error {
	if err := p.analyze(); err != nil {
		return err
	}

	node := p.node.Limit
	if node == nil {
		return nil
	}

	if node.Offset != nil {
		val := node.Offset.(*sqlparser.SQLVal)
		out, err := strconv.ParseInt(hack.String(val.Val), 10, 64)
		if err != nil {
			return err
		}
		p.Offset = int(out)
	}

	if node.Rowcount != nil {
		val := node.Rowcount.(*sqlparser.SQLVal)
		out, err := strconv.ParseInt(hack.String(val.Val), 10, 64)
		if err != nil {
			return err
		}
		p.Limit = int(out)
	}
	p.rewritten = &sqlparser.Limit{Rowcount: sqlparser.NewIntVal([]byte(fmt.Sprintf("%d", p.Offset+p.Limit)))}
	return nil
}

// Type returns the type of the plan.
func (p *LimitPlan) Type() PlanType {
	return p.typ
}

// JSON returns the plan info.
func (p *LimitPlan) JSON() string {
	bout, err := json.MarshalIndent(p, "", "\t")
	if err != nil {
		return err.Error()
	}
	return string(bout)
}

// Children returns the children of the plan.
func (p *LimitPlan) Children() *PlanTree {
	return nil
}

// ReWritten used to re-write the limit clause.
func (p *LimitPlan) ReWritten() *sqlparser.Limit {
	return p.rewritten
}

// Size returns the memory size.
func (p *LimitPlan) Size() int {
	return 0
}
