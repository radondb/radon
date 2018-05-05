/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package executor

import (
	"planner"
	"xcontext"

	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ Executor = &LimitExecutor{}
)

// LimitExecutor represents limit executor.
type LimitExecutor struct {
	log  *xlog.Log
	plan planner.Plan
}

// NewLimitExecutor creates the new limit executor.
func NewLimitExecutor(log *xlog.Log, plan planner.Plan) *LimitExecutor {
	return &LimitExecutor{
		log:  log,
		plan: plan,
	}
}

// Execute used to execute the executor.
func (executor *LimitExecutor) Execute(ctx *xcontext.ResultContext) error {
	rs := ctx.Results
	plan := executor.plan.(*planner.LimitPlan)
	rs.Limit(plan.Offset, plan.Limit)
	return nil
}
