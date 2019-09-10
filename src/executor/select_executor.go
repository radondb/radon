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
	"executor/engine"
	"planner"
	"xcontext"

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
	log := executor.log
	plan := executor.plan.(*planner.SelectPlan)
	planEngine := engine.BuildEngine(log, plan.Root, executor.txn)
	if err := planEngine.Execute(ctx); err != nil {
		return err
	}
	return nil
}
