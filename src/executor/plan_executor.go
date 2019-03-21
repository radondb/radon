package executor

import (
	"backend"
	"planner"
	"xcontext"

	"github.com/xelabs/go-mysqlstack/xlog"
)

// PlanExecutor interface.
type PlanExecutor interface {
	execute(reqCtx *xcontext.RequestContext, ctx *xcontext.ResultContext) error
}

// buildExecutor used to build the executor tree.
func buildExecutor(log *xlog.Log, plan planner.PlanNode, txn backend.Transaction) PlanExecutor {
	var exec PlanExecutor
	switch node := plan.(type) {
	case *planner.MergeNode:
		exec = NewMergeExecutor(log, node, txn)
	case *planner.JoinNode:
		joinExec := NewJoinExecutor(log, node, txn)
		joinExec.left = buildExecutor(log, node.Left, txn)
		joinExec.right = buildExecutor(log, node.Right, txn)
		exec = joinExec
	}
	return exec
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
