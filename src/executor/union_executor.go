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
	_ Executor = &UnionExecutor{}
)

// UnionExecutor represents select executor
type UnionExecutor struct {
	log  *xlog.Log
	plan planner.Plan
	txn  backend.Transaction
}

// NewUnionExecutor creates the new select executor.
func NewUnionExecutor(log *xlog.Log, plan planner.Plan, txn backend.Transaction) *UnionExecutor {
	return &UnionExecutor{
		log:  log,
		plan: plan,
		txn:  txn,
	}
}

// Execute used to execute the executor.
func (executor *UnionExecutor) Execute(ctx *xcontext.ResultContext) error {
	log := executor.log
	plan := executor.plan.(*planner.UnionPlan)
	planEngine := buildEngine(log, plan.Root, executor.txn)
	if err := planEngine.execute(ctx); err != nil {
		return err
	}
	return nil
}
