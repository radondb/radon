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

	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ Executor = &DeleteExecutor{}
)

// DeleteExecutor represents delete executor
type DeleteExecutor struct {
	log  *xlog.Log
	plan planner.Plan
	txn  *backend.Txn
}

// NewDeleteExecutor creates new delete executor.
func NewDeleteExecutor(log *xlog.Log, plan planner.Plan, txn *backend.Txn) *DeleteExecutor {
	return &DeleteExecutor{
		log:  log,
		plan: plan,
		txn:  txn,
	}
}

// Execute used to execute the executor.
func (executor *DeleteExecutor) Execute(ctx *xcontext.ResultContext) error {
	plan := executor.plan.(*planner.DeletePlan)
	reqCtx := xcontext.NewRequestContext()
	reqCtx.Mode = plan.ReqMode
	reqCtx.TxnMode = xcontext.TxnWrite
	reqCtx.Querys = plan.Querys
	reqCtx.RawQuery = plan.RawQuery

	rs, err := executor.txn.Execute(reqCtx)
	if err != nil {
		return err
	}
	ctx.Results = rs
	return nil
}
