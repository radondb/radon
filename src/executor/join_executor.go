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
	_ Executor = &JoinExecutor{}
)

// JoinExecutor tuple.
type JoinExecutor struct {
	log  *xlog.Log
	plan planner.Plan
}

// NewJoinExecutor creates new join executor.
func NewJoinExecutor(log *xlog.Log, plan planner.Plan) *JoinExecutor {
	return &JoinExecutor{
		log:  log,
		plan: plan,
	}
}

// Execute used to execute the executor.
func (executor *JoinExecutor) Execute(ctx *xcontext.ResultContext) error {
	return nil
}
