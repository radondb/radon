/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package operator

import (
	"planner"
	"xcontext"

	"github.com/xelabs/go-mysqlstack/xlog"
)

// Operator interface.
type Operator interface {
	Execute(*xcontext.ResultContext) error
}

// ExecSubPlan used to execute all the children plan.
func ExecSubPlan(log *xlog.Log, node planner.PlanNode, ctx *xcontext.ResultContext) error {
	subPlanTree := node.Children()
	if subPlanTree != nil {
		for _, subPlan := range subPlanTree.Plans() {
			switch subPlan.Type() {
			case planner.PlanTypeAggregate:
				aggrOperator := NewAggregateOperator(log, subPlan)
				if err := aggrOperator.Execute(ctx); err != nil {
					return err
				}
			case planner.PlanTypeOrderby:
				orderByOperator := NewOrderByOperator(log, subPlan)
				if err := orderByOperator.Execute(ctx); err != nil {
					return err
				}
			case planner.PlanTypeLimit:
				limitOperator := NewLimitOperator(log, subPlan)
				if err := limitOperator.Execute(ctx); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
