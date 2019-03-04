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
	_ PlanExecutor = &MergeExecutor{}
)

// MergeExecutor represents merge executor.
type MergeExecutor struct {
	log  *xlog.Log
	node *planner.MergeNode
	txn  backend.Transaction
}

// NewMergeExecutor creates the new merge executor.
func NewMergeExecutor(log *xlog.Log, node *planner.MergeNode, txn backend.Transaction) *MergeExecutor {
	return &MergeExecutor{
		log:  log,
		node: node,
		txn:  txn,
	}
}

// execute used to execute the executor.
func (m *MergeExecutor) execute(reqCtx *xcontext.RequestContext, ctx *xcontext.ResultContext) error {
	var err error
	reqCtx.Querys = m.node.Querys
	subPlanTree := m.node.Children()
	if ctx.Results, err = m.txn.Execute(reqCtx); err != nil {
		return err
	}

	// Execute all the children plan.
	if subPlanTree != nil {
		for _, subPlan := range subPlanTree.Plans() {
			switch subPlan.Type() {
			case planner.PlanTypeAggregate:
				aggrExecutor := NewAggregateExecutor(m.log, subPlan)
				if err := aggrExecutor.Execute(ctx); err != nil {
					return err
				}
			case planner.PlanTypeOrderby:
				orderByExecutor := NewOrderByExecutor(m.log, subPlan)
				if err := orderByExecutor.Execute(ctx); err != nil {
					return err
				}
			case planner.PlanTypeLimit:
				limitExecutor := NewLimitExecutor(m.log, subPlan)
				if err := limitExecutor.Execute(ctx); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
