/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package evaluation

import (
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/expression/datum"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

const (
	// DecimalLongLongDigits decimal longlong digits.
	DecimalLongLongDigits = 22
	// FloatDigits float decimal precision.
	FloatDigits = 6
	// DoubleDigits double decimal precision.
	DoubleDigits = 15
)

// AggrType type.
type AggrType string

const (
	// AggrTypeNull enum.
	AggrTypeNull AggrType = ""

	// AggrTypeCount enum.
	AggrTypeCount AggrType = "COUNT"

	// AggrTypeSum enum.
	AggrTypeSum AggrType = "SUM"

	// AggrTypeMin enum.
	AggrTypeMin AggrType = "MIN"

	// AggrTypeMax enum.
	AggrTypeMax AggrType = "MAX"

	// AggrTypeAvg enum.
	AggrTypeAvg AggrType = "AVG"

	// AggrTypeGroupBy enum.
	AggrTypeGroupBy AggrType = "GROUP BY"
)

// Aggregation operator.
type Aggregation struct {
	distinct   bool
	index      int
	aggrTyp    AggrType
	isPushDown bool
	// prec controls the number of digits.
	prec  int
	eval  Evaluation
	field *querypb.Field
}

// AggEvaluateContext is used to store intermediate result when calculating aggregate functions.
type AggEvaluateContext struct {
	count  int64
	val    sqltypes.Value
	hasErr bool
	// buffer used to store the values when Aggregation.distinct is true.
	buffer *common.HashTable
}

// NewAggregation new an Aggregetion.
func NewAggregation(index int, aggrTyp AggrType, distinct, isPushDown bool, eval Evaluation, field *querypb.Field) *Aggregation {
	return &Aggregation{
		distinct:   distinct,
		index:      index,
		aggrTyp:    aggrTyp,
		isPushDown: isPushDown,
		prec:       -1,
		eval:       eval,
		field:      field,
	}
}

// InitEvalCtx used to init the AggEvaluateContext.
func (aggr *Aggregation) InitEvalCtx(x []sqltypes.Value) *AggEvaluateContext {
	var count int64
	v := sqltypes.MakeTrusted(sqltypes.Null, nil)
	if x != nil {
		v = x[aggr.index]
	}

	buffer := common.NewHashTable()
	if !aggr.isPushDown && v.Type() != sqltypes.Null {
		count = 1
		if aggr.distinct {
			key := v.Raw()
			buffer.Put(key, []byte{})
		}
	}
	return &AggEvaluateContext{
		count:  count,
		val:    v,
		buffer: buffer,
	}
}

// FixField used to fix querypb.Field lenght and decimal.
func (aggr *Aggregation) FixField() {
	if !aggr.isPushDown || aggr.aggrTyp == AggrTypeAvg {
		switch aggr.aggrTyp {
		case AggrTypeMax, AggrTypeMin:
		case AggrTypeCount:
			aggr.field.Decimals = 0
			aggr.field.ColumnLength = 21
			aggr.field.Type = querypb.Type_INT64
		case AggrTypeAvg:
			aggr.field.ColumnLength += 4
			decimals := aggr.field.Decimals + 4
			if sqltypes.IsIntegral(aggr.field.Type) || aggr.field.Type == sqltypes.Decimal {
				if sqltypes.IsUnsigned(aggr.field.Type) {
					aggr.field.ColumnLength++
				}
				if aggr.field.Decimals == 0 {
					aggr.field.ColumnLength++
				}
				if decimals > 30 {
					decimals = 30
				}
				aggr.field.Type = sqltypes.Decimal
			} else if sqltypes.IsFloat(aggr.field.Type) {
				if decimals > 31 {
					decimals = 31
				}
				aggr.field.Type = querypb.Type_FLOAT64
			} else if sqltypes.IsTemporal(aggr.field.Type) {
				aggr.field.Type = sqltypes.Decimal
			} else {
				decimals = 31
				aggr.field.Type = querypb.Type_FLOAT64
			}
			aggr.field.Decimals = decimals
		case AggrTypeSum:
			if sqltypes.IsIntegral(aggr.field.Type) || aggr.field.Type == sqltypes.Decimal {
				aggr.field.ColumnLength += DecimalLongLongDigits
				if sqltypes.IsUnsigned(aggr.field.Type) {
					aggr.field.ColumnLength++
				}
				aggr.field.Type = sqltypes.Decimal
			} else if sqltypes.IsFloat(aggr.field.Type) {
				if aggr.field.Decimals < 31 {
					aggr.field.ColumnLength = aggr.field.ColumnLength + DoubleDigits + 2
				} else {
					aggr.field.ColumnLength = 23
				}
				aggr.field.Type = querypb.Type_FLOAT64
			} else if sqltypes.IsTemporal(aggr.field.Type) {
				aggr.field.Type = sqltypes.Decimal
			} else {
				aggr.field.Decimals = 31
				aggr.field.ColumnLength = 23
				aggr.field.Type = querypb.Type_FLOAT64
			}
		}
	}

	// FLOAT(M,D).
	if aggr.field.Type == sqltypes.Decimal || sqltypes.IsFloat(aggr.field.Type) && aggr.field.Decimals < 31 {
		aggr.prec = int(aggr.field.Decimals)
	}
}

// Update during executing.
func (aggr *Aggregation) Update(x []sqltypes.Value, evalCtx *AggEvaluateContext) {
	if evalCtx.hasErr {
		return
	}

	v := x[aggr.index]
	if v.Type() == sqltypes.Null {
		return
	}

	if !aggr.isPushDown && aggr.distinct {
		key := v.Raw()
		if has, _ := evalCtx.buffer.Get(key); !has {
			evalCtx.buffer.Put(key, []byte{})
		} else {
			return
		}
	}

	var err error
	switch aggr.aggrTyp {
	case AggrTypeMin:
		evalCtx.val = sqltypes.Min(evalCtx.val, v)
	case AggrTypeMax:
		evalCtx.val = sqltypes.Max(evalCtx.val, v)
	case AggrTypeSum:
		evalCtx.count++
		evalCtx.val, err = sqltypes.NullsafeSum(evalCtx.val, v, aggr.field.Type, aggr.prec)
	case AggrTypeCount:
		if aggr.isPushDown {
			evalCtx.val, err = sqltypes.NullsafeAdd(evalCtx.val, v, aggr.field.Type, aggr.prec)
		} else {
			evalCtx.count++
		}
	case AggrTypeAvg:
		if !aggr.isPushDown {
			evalCtx.count++
			evalCtx.val, err = sqltypes.NullsafeSum(evalCtx.val, v, aggr.field.Type, aggr.prec)
		}
	}
	if err != nil {
		evalCtx.hasErr = true
	}
}

// GetResult used to get sqltypes.Value finally.
func (aggr *Aggregation) GetResult(evalCtx *AggEvaluateContext) sqltypes.Value {
	var val sqltypes.Value
	var err error
	if evalCtx.hasErr {
		return sqltypes.MakeTrusted(aggr.field.Type, []byte("0"))
	}
	switch aggr.aggrTyp {
	case AggrTypeAvg:
		if !aggr.isPushDown {
			val, err = sqltypes.NullsafeDiv(evalCtx.val, sqltypes.NewInt64(evalCtx.count), aggr.field.Type, aggr.prec)
		}
	case AggrTypeMax, AggrTypeMin:
		val = evalCtx.val
	case AggrTypeSum:
		val, err = sqltypes.Cast(evalCtx.val, aggr.field.Type)
	case AggrTypeCount:
		if aggr.isPushDown {
			val = evalCtx.val
		} else {
			val = sqltypes.NewInt64(evalCtx.count)
		}
	}
	if err != nil {
		val = sqltypes.MakeTrusted(aggr.field.Type, []byte("0"))
	}
	return val
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
func GetResults(aggrs []*Aggregation, evalCtxs []*AggEvaluateContext, x []sqltypes.Value) ([]sqltypes.Value, []int, error) {
	var deIdxs []int
	i := 0
	for i < len(aggrs) {
		aggr := aggrs[i]
		evalCtx := evalCtxs[i]
		if aggr.isPushDown && aggr.aggrTyp == AggrTypeAvg {
			var err error
			if x[aggr.index], err = sqltypes.NullsafeDiv(evalCtxs[i+1].val, evalCtxs[i+2].val, aggr.field.Type, aggr.prec); err != nil {
				x[aggr.index] = sqltypes.MakeTrusted(aggr.field.Type, []byte("0"))
			}
			deIdxs = append(deIdxs, aggr.index+1)
			i = i + 2
		} else {
			x[aggr.index] = aggr.GetResult(evalCtx)
		}

		if aggr.eval != nil {
			fields := map[string]*querypb.Field{
				"`tmp_aggr`": aggr.field,
			}
			f, err := aggr.eval.FixField(fields)
			if err != nil {
				return x, deIdxs, err
			}

			d, err := datum.ValToDatum(x[aggr.index])
			if err != nil {
				return x, deIdxs, err
			}
			values := map[string]datum.Datum{
				"`tmp_aggr`": d,
			}
			r, err := aggr.eval.Update(values)
			if err != nil {
				return x, deIdxs, err
			}

			x[aggr.index], err = datum.DatumToVal(r, f)
			if err != nil {
				return x, deIdxs, err
			}
		}
		i++
	}

	return x, deIdxs, nil
}
