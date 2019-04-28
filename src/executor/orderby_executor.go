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
	"sort"
	"xcontext"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
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
	var err error
	rs := ctx.Results
	plan := executor.plan.(*planner.OrderByPlan)

	sort.Slice(rs.Rows, func(i, j int) bool {
		// If there are any errors below, the function sets
		// the external err and returns true. Once err is set,
		// all subsequent calls return true. This will make
		// Slice think that all elements are in the correct
		// order and return more quickly.
		for _, orderby := range plan.OrderBys {
			if err != nil {
				return true
			}

			idx := -1
			for k, f := range rs.Fields {
				if f.Name == orderby.Field && (orderby.Table == "" || orderby.Table == f.Table) {
					idx = k
					break
				}
			}
			if idx == -1 {
				err = errors.Errorf("can.not.find.the.orderby.field[%s].direction.asc", orderby.Field)
				return true
			}

			cmp := sqltypes.NullsafeCompare(rs.Rows[i][idx], rs.Rows[j][idx])
			if cmp == 0 {
				continue
			}
			if orderby.Direction == planner.DESC {
				cmp = -cmp
			}
			return cmp < 0
		}
		return true
	})

	return err
}
