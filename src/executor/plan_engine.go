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

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// PlanEngine interface.
type PlanEngine interface {
	execute(ctx *xcontext.ResultContext) error
	execBindVars(ctx *xcontext.ResultContext, bindVars map[string]*querypb.BindVariable, wantfields bool) error
	getFields(ctx *xcontext.ResultContext, bindVars map[string]*querypb.BindVariable) error
}

// buildEngine used to build the executor tree.
func buildEngine(log *xlog.Log, plan planner.PlanNode, txn backend.Transaction) PlanEngine {
	var engine PlanEngine
	switch node := plan.(type) {
	case *planner.MergeNode:
		engine = NewMergeEngine(log, node, txn)
	case *planner.JoinNode:
		joinEngine := NewJoinEngine(log, node, txn)
		joinEngine.left = buildEngine(log, node.Left, txn)
		joinEngine.right = buildEngine(log, node.Right, txn)
		engine = joinEngine
	case *planner.UnionNode:
		unionEngine := NewUnionEngine(log, node, txn)
		unionEngine.left = buildEngine(log, node.Left, txn)
		unionEngine.right = buildEngine(log, node.Right, txn)
		engine = unionEngine
	}
	return engine
}

// execSubPlan used to execute all the children plan.
func execSubPlan(log *xlog.Log, node planner.PlanNode, ctx *xcontext.ResultContext) error {
	subPlanTree := node.Children()
	if subPlanTree != nil {
		for _, subPlan := range subPlanTree.Plans() {
			switch subPlan.Type() {
			case planner.PlanTypeAggregate:
				aggrExecutor := NewAggregateExecutor(log, subPlan)
				if err := aggrExecutor.Execute(ctx); err != nil {
					return err
				}
			case planner.PlanTypeOrderby:
				orderByExecutor := NewOrderByExecutor(log, subPlan)
				if err := orderByExecutor.Execute(ctx); err != nil {
					return err
				}
			case planner.PlanTypeLimit:
				limitExecutor := NewLimitExecutor(log, subPlan)
				if err := limitExecutor.Execute(ctx); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
