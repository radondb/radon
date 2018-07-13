/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ Plan = &DistinctPlan{}
)

// DistinctPlan represents distinct plan.
type DistinctPlan struct {
	log *xlog.Log

	node *sqlparser.Select

	// type
	Typ PlanType
}

// NewDistinctPlan used to create DistinctPlan.
func NewDistinctPlan(log *xlog.Log, node *sqlparser.Select) *DistinctPlan {
	return &DistinctPlan{
		log:  log,
		node: node,
		Typ:  PlanTypeDistinct,
	}
}

// analyze used to check the distinct is at the support level.
// Unsupported:
// 1. all distinct clause.
func (p *DistinctPlan) analyze() error {
	node := p.node
	if node.Distinct != "" {
		return errors.New("unsupported: distinct")
	}
	return nil
}

// Build used to build distributed querys.
func (p *DistinctPlan) Build() error {
	return p.analyze()
}

// Type returns the type of the plan.
func (p *DistinctPlan) Type() PlanType {
	return p.Typ
}

// JSON returns the plan info.
func (p *DistinctPlan) JSON() string {
	return ""
}

// Children returns the children of the plan.
func (p *DistinctPlan) Children() *PlanTree {
	return nil
}

// Size returns the memory size.
func (p *DistinctPlan) Size() int {
	return 0
}
