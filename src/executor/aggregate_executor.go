/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package executor

import (
	"expression"
	"planner"
	"xcontext"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ Executor = &AggregateExecutor{}
)

// AggregateExecutor represents aggregate executor.
// Including: COUNT/MAX/MIN/SUM/AVG/GROUPBY.
type AggregateExecutor struct {
	log  *xlog.Log
	plan planner.Plan
}

// NewAggregateExecutor creates new AggregateExecutor.
func NewAggregateExecutor(log *xlog.Log, plan planner.Plan) *AggregateExecutor {
	return &AggregateExecutor{
		log:  log,
		plan: plan,
	}
}

// Execute used to execute the executor.
func (executor *AggregateExecutor) Execute(ctx *xcontext.ResultContext) error {
	rs := ctx.Results
	executor.aggregate(rs)
	return nil
}

// Aggregate used to do rows-aggregator(COUNT/SUM/MIN/MAX/AVG) and grouped them into group-by fields.
func (executor *AggregateExecutor) aggregate(result *sqltypes.Result) {
	var deIdxs []int
	plan := executor.plan.(*planner.AggregatePlan)
	if plan.Empty() {
		return
	}
	aggPlans := plan.NormalAggregators()
	aggPlansLen := len(aggPlans)
	groupAggrs := plan.GroupAggregators()

	type group struct {
		row      []sqltypes.Value
		evalCtxs []*expression.AggEvaluateContext
	}

	aggrs := expression.NewAggregations(aggPlans, plan.IsPushDown, result.Fields)
	groups := make(map[string]group)
	for _, row := range result.Rows {
		keySlice := []byte{0x01}
		for _, v := range groupAggrs {
			keySlice = append(keySlice, row[v.Index].Raw()...)
			keySlice = append(keySlice, 0x02)
		}
		key := common.BytesToString(keySlice)
		if g, ok := groups[key]; !ok {
			evalCtxs := expression.NewAggEvalCtxs(aggrs, row)
			groups[key] = group{row, evalCtxs}
		} else {
			if aggPlansLen > 0 {
				for i, aggr := range aggrs {
					aggr.Update(row, g.evalCtxs[i])
				}
			}
		}
	}

	// Handle the avg operator and rebuild the results.
	i := 0
	result.Rows = make([][]sqltypes.Value, len(groups))
	for _, g := range groups {
		result.Rows[i], deIdxs = expression.GetResults(aggrs, g.evalCtxs, g.row)
		i++
	}

	if len(groups) == 0 && aggPlansLen > 0 {
		result.Rows = make([][]sqltypes.Value, 1)
		evalCtxs := expression.NewAggEvalCtxs(aggrs, nil)
		result.Rows[0], deIdxs = expression.GetResults(aggrs, evalCtxs, make([]sqltypes.Value, len(result.Fields)))
	}
	// Remove avg decompose columns.
	result.RemoveColumns(deIdxs...)
}
