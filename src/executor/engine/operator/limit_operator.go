/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package operator

import (
	"planner"
	"xcontext"

	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ Operator = &LimitOperator{}
)

// LimitOperator represents limit operator.
type LimitOperator struct {
	log  *xlog.Log
	plan planner.Plan
}

// NewLimitOperator creates the new limit operator.
func NewLimitOperator(log *xlog.Log, plan planner.Plan) *LimitOperator {
	return &LimitOperator{
		log:  log,
		plan: plan,
	}
}

// Execute used to execute the operator.
func (operator *LimitOperator) Execute(ctx *xcontext.ResultContext) error {
	rs := ctx.Results
	plan := operator.plan.(*planner.LimitPlan)
	rs.Limit(plan.Offset, plan.Limit)
	return nil
}
