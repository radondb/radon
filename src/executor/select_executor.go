/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package executor

import (
	"backend"
	"planner"
	"xcontext"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ Executor = &SelectExecutor{}
)

// SelectExecutor represents select executor
type SelectExecutor struct {
	log  *xlog.Log
	plan planner.Plan
	txn  backend.Transaction
}

// NewSelectExecutor creates the new select executor.
func NewSelectExecutor(log *xlog.Log, plan planner.Plan, txn backend.Transaction) *SelectExecutor {
	return &SelectExecutor{
		log:  log,
		plan: plan,
		txn:  txn,
	}
}

// Execute used to execute the executor.
func (executor *SelectExecutor) Execute(ctx *xcontext.ResultContext) error {
	var err error
	plan := executor.plan.(*planner.SelectPlan)
	subPlanTree := plan.Children()
	reqCtx := xcontext.NewRequestContext()
	reqCtx.Mode = plan.ReqMode
	reqCtx.TxnMode = xcontext.TxnRead
	reqCtx.Querys = plan.Querys
	reqCtx.RawQuery = plan.RawQuery

	// Execute the parent plan.
	if ctx.Results, err = executor.txn.Execute(reqCtx); err != nil {
		return err
	}

	// Execute all the children plan.
	if subPlanTree != nil {
		for _, subPlan := range subPlanTree.Plans() {
			switch subPlan.Type() {
			case planner.PlanTypeAggregate:
				aggrExecutor := NewAggregateExecutor(executor.log, subPlan)
				if err := aggrExecutor.Execute(ctx); err != nil {
					return err
				}
			case planner.PlanTypeOrderby:
				orderByExecutor := NewOrderByExecutor(executor.log, subPlan)
				if err := orderByExecutor.Execute(ctx); err != nil {
					return err
				}
			case planner.PlanTypeLimit:
				limitExecutor := NewLimitExecutor(executor.log, subPlan)
				if err := limitExecutor.Execute(ctx); err != nil {
					return err
				}
			case planner.PlanTypeDistinct:
			default:
				return errors.Errorf("unsupported.execute.type:%v", plan.Type())
			}
		}
	}
	return nil
}
