/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package expression

import (
	"planner"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// Aggregation operator.
type Aggregation struct {
	distinct   bool
	index      int
	aggrTyp    planner.AggrType
	fieldType  querypb.Type
	isPushDown bool
	// prec controls the number of digits.
	prec int
}

// AggEvaluateContext is used to store intermediate result when calculating aggregate functions.
type AggEvaluateContext struct {
	count int64
	val   sqltypes.Value
	// buffer used to store the value maps when Aggregation.distinct is true.
	buffer map[string]sqltypes.Value
}

// NewAggregation new an Aggregetion.
func newAggregation(p planner.Aggregator, isPushDown bool) *Aggregation {
	return &Aggregation{
		distinct:   p.Distinct,
		index:      p.Index,
		aggrTyp:    p.Type,
		isPushDown: isPushDown,
		prec:       -1,
	}
}

// InitEvalCtx used to init the AggEvaluateContext.
func (aggr *Aggregation) InitEvalCtx(x []sqltypes.Value) *AggEvaluateContext {
	var count int64
	v := sqltypes.MakeTrusted(sqltypes.Null, nil)
	if x != nil {
		v = x[aggr.index]
	}

	buffer := make(map[string]sqltypes.Value)
	if !aggr.isPushDown && v.Type() != sqltypes.Null {
		count = 1
		if aggr.distinct {
			keySlice := []byte{0x01}
			keySlice = append(keySlice, v.Raw()...)
			keySlice = append(keySlice, 0x02)
			key := common.BytesToString(keySlice)
			buffer[key] = v
		}
	}
	return &AggEvaluateContext{
		count:  count,
		val:    v,
		buffer: buffer,
	}
}

// FixField used to fix querypb.Field lenght and decimal.
func (aggr *Aggregation) FixField(field *querypb.Field) {
	if !aggr.isPushDown || aggr.aggrTyp == planner.AggrTypeAvg {
		switch aggr.aggrTyp {
		case planner.AggrTypeMax, planner.AggrTypeMin:
		case planner.AggrTypeCount:
			field.Decimals = 0
			field.ColumnLength = 21
			field.Type = querypb.Type_INT64
		case planner.AggrTypeAvg:
			field.ColumnLength = field.ColumnLength + 4
			decimals := field.Decimals + 4
			if sqltypes.IsIntegral(field.Type) || field.Type == sqltypes.Decimal {
				if sqltypes.IsUnsigned(field.Type) {
					field.ColumnLength++
				}
				if field.Decimals == 0 {
					field.ColumnLength++
				}
				if decimals > 30 {
					decimals = 30
				}
				field.Type = sqltypes.Decimal
			} else if sqltypes.IsFloat(field.Type) {
				if decimals > 31 {
					decimals = 31
				}
				field.Type = querypb.Type_FLOAT64
			} else {
				decimals = 31
				field.Type = querypb.Type_FLOAT64
			}
			field.Decimals = decimals
		case planner.AggrTypeSum:
			if sqltypes.IsIntegral(field.Type) || field.Type == sqltypes.Decimal {
				field.ColumnLength = field.ColumnLength + DecimalLongLongDigits
				if sqltypes.IsUnsigned(field.Type) {
					field.ColumnLength++
				}
				field.Type = sqltypes.Decimal
			} else if sqltypes.IsFloat(field.Type) {
				if field.Decimals < 31 {
					field.ColumnLength = field.ColumnLength + DoubleDigits + 2
				} else {
					field.ColumnLength = 23
				}
				field.Type = querypb.Type_FLOAT64
			} else {
				field.Decimals = 31
				field.ColumnLength = 23
				field.Type = querypb.Type_FLOAT64
			}
		}
	}

	if field.Type == sqltypes.Decimal {
		aggr.prec = int(field.Decimals)
	}
	aggr.fieldType = field.Type
}

// Update during executing.
func (aggr *Aggregation) Update(x []sqltypes.Value, evalCtx *AggEvaluateContext) {
	v := x[aggr.index]
	if v.Type() == sqltypes.Null {
		return
	}

	if !aggr.isPushDown && aggr.distinct {
		keySlice := []byte{0x01}
		keySlice = append(keySlice, v.Raw()...)
		keySlice = append(keySlice, 0x02)
		key := common.BytesToString(keySlice)
		if _, ok := evalCtx.buffer[key]; !ok {
			evalCtx.buffer[key] = v
		} else {
			return
		}
	}
	switch aggr.aggrTyp {
	case planner.AggrTypeMin:
		evalCtx.val = sqltypes.Min(evalCtx.val, v)
	case planner.AggrTypeMax:
		evalCtx.val = sqltypes.Max(evalCtx.val, v)
	case planner.AggrTypeSum:
		evalCtx.count++
		evalCtx.val, _ = sqltypes.NullsafeAdd(evalCtx.val, v, aggr.fieldType, aggr.prec)
	case planner.AggrTypeCount:
		if aggr.isPushDown {
			evalCtx.val, _ = sqltypes.NullsafeAdd(evalCtx.val, v, aggr.fieldType, aggr.prec)
		} else {
			evalCtx.count++
		}
	case planner.AggrTypeAvg:
		if !aggr.isPushDown {
			evalCtx.count++
			evalCtx.val, _ = sqltypes.NullsafeAdd(evalCtx.val, v, aggr.fieldType, aggr.prec)
		}
	}
}

// GetResult used to get Value finally.
func (aggr *Aggregation) GetResult(evalCtx *AggEvaluateContext) sqltypes.Value {
	var val sqltypes.Value
	switch aggr.aggrTyp {
	case planner.AggrTypeAvg:
		if !aggr.isPushDown {
			val, _ = sqltypes.NullsafeDiv(evalCtx.val, sqltypes.NewInt64(evalCtx.count), aggr.fieldType, aggr.prec)
		}
	case planner.AggrTypeMax, planner.AggrTypeMin:
		val = evalCtx.val
	case planner.AggrTypeSum:
		var err error
		if val, err = sqltypes.Cast(evalCtx.val, aggr.fieldType); err != nil {
			val = sqltypes.MakeTrusted(aggr.fieldType, []byte("0"))
		}
	case planner.AggrTypeCount:
		if aggr.isPushDown {
			val = evalCtx.val
		} else {
			val = sqltypes.NewInt64(evalCtx.count)
		}
	}
	return val
}

// NewAggregations new aggrs based on plans.
func NewAggregations(plans []planner.Aggregator, isPushDown bool, fields []*querypb.Field) []*Aggregation {
	var aggrs []*Aggregation
	for _, plan := range plans {
		aggr := newAggregation(plan, isPushDown)
		aggr.FixField(fields[aggr.index])
		aggrs = append(aggrs, aggr)
	}
	return aggrs
}

// NewAggEvalCtxs new evalCtxs.
func NewAggEvalCtxs(aggrs []*Aggregation, x []sqltypes.Value) []*AggEvaluateContext {
	var evalCtxs []*AggEvaluateContext
	for _, aggr := range aggrs {
		evalCtx := aggr.InitEvalCtx(x)
		evalCtxs = append(evalCtxs, evalCtx)
	}
	return evalCtxs
}

// GetResults will be called when all data have been processed.
func GetResults(aggrs []*Aggregation, evalCtxs []*AggEvaluateContext, x []sqltypes.Value) ([]sqltypes.Value, []int) {
	var deIdxs []int
	i := 0
	for i < len(aggrs) {
		aggr := aggrs[i]
		evalCtx := evalCtxs[i]
		if aggr.isPushDown && aggr.aggrTyp == planner.AggrTypeAvg {
			x[aggr.index], _ = sqltypes.NullsafeDiv(evalCtxs[i+1].val, evalCtxs[i+2].val, aggr.fieldType, aggr.prec)
			deIdxs = append(deIdxs, aggr.index+1)
			i = i + 2
		} else {
			x[aggr.index] = aggr.GetResult(evalCtx)
		}
		i++
	}

	return x, deIdxs
}
