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
	aggs := plan.NormalAggregators()
	aggrLen := len(aggs)
	groupAggrs := plan.GroupAggregators()

	type group struct {
		row      []sqltypes.Value
		evalCtxs []*expression.AggEvaluateContext
	}

	aggrs := expression.NewAggregations(aggs, plan.IsPushDown, result.Fields)
	groups := make(map[string]group)
	for _, row1 := range result.Rows {
		keySlice := []byte{0x01}
		for _, v := range groupAggrs {
			keySlice = append(keySlice, row1[v.Index].Raw()...)
			keySlice = append(keySlice, 0x02)
		}
		key := common.BytesToString(keySlice)
		if g, ok := groups[key]; !ok {
			evalCtxs := expression.NewAggEvalCtxs(aggrs, row1)
			groups[key] = group{row1, evalCtxs}
		} else {
			if aggrLen > 0 {
				for i, aggr := range aggrs {
					aggr.Update(row1, g.evalCtxs[i])
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

	if len(groups) == 0 && aggrLen > 0 {
		result.Rows = make([][]sqltypes.Value, 1)
		evalCtxs := expression.NewAggEvalCtxs(aggrs, nil)
		result.Rows[0], deIdxs = expression.GetResults(aggrs, evalCtxs, make([]sqltypes.Value, len(aggrs)))
	}
	// Remove avg decompose columns.
	result.RemoveColumns(deIdxs...)
}
