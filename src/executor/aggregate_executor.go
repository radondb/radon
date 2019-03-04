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

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/hack"
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
	aggrs := plan.NormalAggregators()
	aggrLen := len(aggrs)
	groupAggrs := plan.GroupAggregators()

	groups := make(map[string][]sqltypes.Value)
	for _, row1 := range result.Rows {
		keySlice := []byte{0x01}
		for _, v := range groupAggrs {
			keySlice = append(keySlice, row1[v.Index].Raw()...)
			keySlice = append(keySlice, 0x02)
		}
		key := hack.String(keySlice)
		if row2, ok := groups[key]; !ok {
			groups[key] = row1
		} else {
			if aggrLen > 0 {
				groups[key] = operator(aggrs, row1)(row2)
			}
		}
	}

	// Handle the avg operator and rebuild the results.
	i := 0
	result.Rows = make([][]sqltypes.Value, len(groups))
	for _, v := range groups {
		for _, aggr := range aggrs {
			switch aggr.Type {
			case planner.AggrTypeAvg:
				v1, v2 := v[aggr.Index], v[aggr.Index+1]
				v[aggr.Index] = sqltypes.Operator(v1, v2, sqltypes.DivFn)
				deIdxs = append(deIdxs, aggr.Index+1)
			}
		}
		result.Rows[i] = v
		i++
	}

	// Remove avg decompose columns.
	result.RemoveColumns(deIdxs...)
}

// aggregate supported type: SUM/COUNT/MIN/MAX/AVG.
func operator(aggrs []planner.Aggregator, x []sqltypes.Value) func([]sqltypes.Value) []sqltypes.Value {
	return func(y []sqltypes.Value) []sqltypes.Value {
		ret := sqltypes.Row(x).Copy()
		for _, aggr := range aggrs {
			switch aggr.Type {
			case planner.AggrTypeSum, planner.AggrTypeCount:
				v1, v2 := x[aggr.Index], y[aggr.Index]
				if v1.Type() == sqltypes.Null {
					ret[aggr.Index] = v2
				} else if v2.Type() == sqltypes.Null {
					ret[aggr.Index] = v1
				} else {
					ret[aggr.Index] = sqltypes.Operator(v1, v2, sqltypes.SumFn)
				}
			case planner.AggrTypeMin:
				v1, v2 := x[aggr.Index], y[aggr.Index]
				if v1.Type() == sqltypes.Null {
					ret[aggr.Index] = v2
				} else if v2.Type() == sqltypes.Null {
					ret[aggr.Index] = v1
				} else {
					ret[aggr.Index] = sqltypes.Operator(v1, v2, sqltypes.MinFn)
				}
			case planner.AggrTypeMax:
				v1, v2 := x[aggr.Index], y[aggr.Index]
				if v1.Type() == sqltypes.Null {
					ret[aggr.Index] = v2
				} else if v2.Type() == sqltypes.Null {
					ret[aggr.Index] = v1
				} else {
					ret[aggr.Index] = sqltypes.Operator(v1, v2, sqltypes.MaxFn)
				}
			case planner.AggrTypeAvg:
				// nop
			}
		}
		return ret
	}
}
