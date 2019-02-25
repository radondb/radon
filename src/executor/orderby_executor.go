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

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ Executor = &OrderByExecutor{}
)

// OrderByExecutor represents order by executor.
type OrderByExecutor struct {
	log  *xlog.Log
	plan planner.Plan
}

// NewOrderByExecutor creates new orderby executor.
func NewOrderByExecutor(log *xlog.Log, plan planner.Plan) *OrderByExecutor {
	return &OrderByExecutor{
		log:  log,
		plan: plan,
	}
}

// Execute used to execute the executor.
func (executor *OrderByExecutor) Execute(ctx *xcontext.ResultContext) error {
	rs := ctx.Results
	plan := executor.plan.(*planner.OrderByPlan)

	for _, orderby := range plan.OrderBys {
		switch orderby.Direction {
		case planner.ASC:
			if err := rs.OrderedByAsc(orderby.Table, orderby.Field); err != nil {
				return errors.WithStack(err)
			}
		case planner.DESC:
			if err := rs.OrderedByDesc(orderby.Table, orderby.Field); err != nil {
				return errors.WithStack(err)
			}
		}
	}
	rs.Sort()
	return nil
}
