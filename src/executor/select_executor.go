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
	reqCtx := xcontext.NewRequestContext()
	reqCtx.Mode = plan.ReqMode
	reqCtx.TxnMode = xcontext.TxnRead
	reqCtx.RawQuery = plan.RawQuery

	switch node := plan.Root.(type) {
	case *planner.MergeNode:
		mergeExecutor := NewMergeExecutor(log, node, executor.txn)
		if err := mergeExecutor.execute(reqCtx, ctx); err != nil {
			return err
		}
	case *planner.JoinNode:
		joinExecutor := NewJoinExecutor(log, node, executor.txn)
		if err := joinExecutor.execute(reqCtx, ctx); err != nil {
			return err
		}
	}
	return nil
}
