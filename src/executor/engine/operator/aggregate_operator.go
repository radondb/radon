/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package operator

import (
	"sort"

	"planner/builder"
	"xcontext"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ Operator = &AggregateOperator{}
)

// AggregateOperator represents aggregate operator.
// Including: COUNT/MAX/MIN/SUM/AVG/GROUPBY.
type AggregateOperator struct {
	log  *xlog.Log
	plan builder.ChildPlan
}

// NewAggregateOperator creates new AggregateOperator.
func NewAggregateOperator(log *xlog.Log, plan builder.ChildPlan) *AggregateOperator {
	return &AggregateOperator{
		log:  log,
		plan: plan,
	}
}

// Execute used to execute the operator.
func (operator *AggregateOperator) Execute(ctx *xcontext.ResultContext) error {
	rs := ctx.Results
	operator.aggregate(rs)
	return nil
}

// Aggregate used to do rows-aggregator(COUNT/SUM/MIN/MAX/AVG) and grouped them into group-by fields.
// Don't use `group by` alone, `group by` needs to be used with the aggregation function. Otherwise
// the result of radon may be different from the result of mysql.
// eg: select a,b from tb group by b.        ×
//     select count(a),b from tb group by b. √
//     select b from tb group by b.          √
func (operator *AggregateOperator) aggregate(result *sqltypes.Result) {
	var deIdxs []int
	plan := operator.plan.(*builder.AggregatePlan)
	if plan.Empty() {
		return
	}

	aggPlans := plan.NormalAggregators()
	aggPlansLen := len(aggPlans)
	groupAggrs := plan.GroupAggregators()
	if len(groupAggrs) > 0 {
		sort.Slice(result.Rows, func(i, j int) bool {
			for _, key := range groupAggrs {
				cmp := sqltypes.NullsafeCompare(result.Rows[i][key.Index], result.Rows[j][key.Index])
				if cmp == 0 {
					continue
				}
				return cmp < 0
			}
			return true
		})
	}

	type group struct {
		row      []sqltypes.Value
		evalCtxs []*sqltypes.AggEvaluateContext
	}

	var aggrs []*sqltypes.Aggregation
	for _, aggPlan := range aggPlans {
		aggr := sqltypes.NewAggregation(aggPlan.Index, aggPlan.Type, aggPlan.Distinct, plan.IsPushDown)
		aggr.FixField(result.Fields[aggPlan.Index])
		aggrs = append(aggrs, aggr)
	}

	var groups []*group
	for _, row := range result.Rows {
		length := len(groups)
		if length == 0 {
			evalCtxs := sqltypes.NewAggEvalCtxs(aggrs, row)
			groups = append(groups, &group{row, evalCtxs})
			continue
		}

		equal := keysEqual(groups[length-1].row, row, groupAggrs)
		if equal {
			if aggPlansLen > 0 {
				for i, aggr := range aggrs {
					aggr.Update(row, groups[length-1].evalCtxs[i])
				}
			}
		} else {
			evalCtxs := sqltypes.NewAggEvalCtxs(aggrs, row)
			groups = append(groups, &group{row, evalCtxs})
		}
	}

	// Handle the avg operator and rebuild the results.
	i := 0
	result.Rows = make([][]sqltypes.Value, len(groups))
	for _, g := range groups {
		result.Rows[i], deIdxs = sqltypes.GetResults(aggrs, g.evalCtxs, g.row)
		i++
	}

	if len(groups) == 0 && aggPlansLen > 0 {
		result.Rows = make([][]sqltypes.Value, 1)
		evalCtxs := sqltypes.NewAggEvalCtxs(aggrs, nil)
		result.Rows[0], deIdxs = sqltypes.GetResults(aggrs, evalCtxs, make([]sqltypes.Value, len(result.Fields)))
	}
	// Remove avg decompose columns.
	result.RemoveColumns(deIdxs...)
}

func keysEqual(row1, row2 []sqltypes.Value, groups []builder.Aggregator) bool {
	for _, v := range groups {
		cmp := sqltypes.NullsafeCompare(row1[v.Index], row2[v.Index])
		if cmp != 0 {
			return false
		}
	}
	return true
}
