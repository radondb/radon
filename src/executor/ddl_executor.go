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
	_ Executor = &DDLExecutor{}
)

// DDLExecutor represents a CREATE, ALTER, DROP executor
type DDLExecutor struct {
	log  *xlog.Log
	plan planner.Plan
	txn  *backend.Txn
}

// NewDDLExecutor creates DDL executor.
func NewDDLExecutor(log *xlog.Log, plan planner.Plan, txn *backend.Txn) *DDLExecutor {
	return &DDLExecutor{
		log:  log,
		plan: plan,
		txn:  txn,
	}
}

// Execute used to execute the executor.
func (executor *DDLExecutor) Execute(ctx *xcontext.ResultContext) error {
	plan := executor.plan.(*planner.DDLPlan)
	reqCtx := xcontext.NewRequestContext()
	reqCtx.Mode = plan.ReqMode
	reqCtx.Querys = plan.Querys
	reqCtx.RawQuery = plan.RawQuery

	res, err := executor.txn.Execute(reqCtx)
	if err != nil {
		return err
	}
	ctx.Results = res
	return nil
}
