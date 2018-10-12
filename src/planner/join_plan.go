/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"errors"

	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ Plan = &JoinPlan{}
)

// JoinPlan represents join plan.
type JoinPlan struct {
	log *xlog.Log

	node *sqlparser.Select

	// type
	typ PlanType
}

// NewJoinPlan used to create JoinPlan.
func NewJoinPlan(log *xlog.Log, node *sqlparser.Select) *JoinPlan {
	return &JoinPlan{
		log:  log,
		node: node,
		typ:  PlanTypeJoin,
	}
}

// analyze used to check the join is at the support level.
// unsupported join.
func (p *JoinPlan) analyze() error {
	node := p.node
	for _, tab := range node.From {
		switch tab.(type) {
		case *sqlparser.AliasedTableExpr:
			// select * from a,b where a.id=b.id
			if len(node.From) > 1 && node.Where != nil {
				return errors.New("unsupported: JOIN.expression")
			}
		}
	}
	return nil
}

// Build used to build distributed querys.
func (p *JoinPlan) Build() error {
	return p.analyze()
}

// Type returns the type of the plan.
func (p *JoinPlan) Type() PlanType {
	return p.typ
}

// JSON returns the plan info.
func (p *JoinPlan) JSON() string {
	return ""
}

// Children returns the children of the plan.
func (p *JoinPlan) Children() *PlanTree {
	return nil
}

// Size returns the memory size.
func (p *JoinPlan) Size() int {
	return 0
}
