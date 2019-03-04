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
	_ PlanExecutor = &JoinExecutor{}
)

// JoinExecutor represents join executor.
type JoinExecutor struct {
	log  *xlog.Log
	node *planner.JoinNode
	txn  backend.Transaction
}

// NewJoinExecutor creates the new join executor.
func NewJoinExecutor(log *xlog.Log, node *planner.JoinNode, txn backend.Transaction) *JoinExecutor {
	return &JoinExecutor{
		log:  log,
		node: node,
		txn:  txn,
	}
}

// execute used to execute the executor.
func (m *JoinExecutor) execute(reqCtx *xcontext.RequestContext, ctx *xcontext.ResultContext) error {
	return errors.New("unsupported: join")
}
