/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package sqltypes

import (
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
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
	fieldType  querypb.Type
	isPushDown bool
	// prec controls the number of digits.
	prec int
}

// AggEvaluateContext is used to store intermediate result when calculating aggregate functions.
type AggEvaluateContext struct {
	count  int64
	val    Value
	hasErr bool
	// buffer used to store the values when Aggregation.distinct is true.
	buffer *common.HashTable
}

// NewAggregation new an Aggregetion.
func NewAggregation(index int, aggrTyp AggrType, distinct, isPushDown bool) *Aggregation {
	return &Aggregation{
		distinct:   distinct,
		index:      index,
		aggrTyp:    aggrTyp,
		isPushDown: isPushDown,
		prec:       -1,
	}
}

// InitEvalCtx used to init the AggEvaluateContext.
func (aggr *Aggregation) InitEvalCtx(x []Value) *AggEvaluateContext {
	var count int64
	v := MakeTrusted(Null, nil)
	if x != nil {
		v = x[aggr.index]
	}

	buffer := common.NewHashTable()
	if !aggr.isPushDown && v.Type() != Null {
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
func (aggr *Aggregation) FixField(field *querypb.Field) {
	if !aggr.isPushDown || aggr.aggrTyp == AggrTypeAvg {
		switch aggr.aggrTyp {
		case AggrTypeMax, AggrTypeMin:
		case AggrTypeCount:
			field.Decimals = 0
			field.ColumnLength = 21
			field.Type = querypb.Type_INT64
		case AggrTypeAvg:
			field.ColumnLength = field.ColumnLength + 4
			decimals := field.Decimals + 4
			if IsIntegral(field.Type) || field.Type == Decimal {
				if IsUnsigned(field.Type) {
					field.ColumnLength++
				}
				if field.Decimals == 0 {
					field.ColumnLength++
				}
				if decimals > 30 {
					decimals = 30
				}
				field.Type = Decimal
			} else if IsFloat(field.Type) {
				if decimals > 31 {
					decimals = 31
				}
				field.Type = querypb.Type_FLOAT64
			} else if IsTemporal(field.Type) {
				field.Type = Decimal
			} else {
				decimals = 31
				field.Type = querypb.Type_FLOAT64
			}
			field.Decimals = decimals
		case AggrTypeSum:
			if IsIntegral(field.Type) || field.Type == Decimal {
				field.ColumnLength = field.ColumnLength + DecimalLongLongDigits
				if IsUnsigned(field.Type) {
					field.ColumnLength++
				}
				field.Type = Decimal
			} else if IsFloat(field.Type) {
				if field.Decimals < 31 {
					field.ColumnLength = field.ColumnLength + DoubleDigits + 2
				} else {
					field.ColumnLength = 23
				}
				field.Type = querypb.Type_FLOAT64
			} else if IsTemporal(field.Type) {
				field.Type = Decimal
			} else {
				field.Decimals = 31
				field.ColumnLength = 23
				field.Type = querypb.Type_FLOAT64
			}
		}
	}

	// FLOAT(M,D).
	if field.Type == Decimal || IsFloat(field.Type) && field.Decimals < 31 {
		aggr.prec = int(field.Decimals)
	}
	aggr.fieldType = field.Type
}

// Update during executing.
func (aggr *Aggregation) Update(x []Value, evalCtx *AggEvaluateContext) {
	if evalCtx.hasErr {
		return
	}

	v := x[aggr.index]
	if v.Type() == Null {
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
		evalCtx.val = Min(evalCtx.val, v)
	case AggrTypeMax:
		evalCtx.val = Max(evalCtx.val, v)
	case AggrTypeSum:
		evalCtx.count++
		evalCtx.val, err = NullsafeSum(evalCtx.val, v, aggr.fieldType, aggr.prec)
	case AggrTypeCount:
		if aggr.isPushDown {
			evalCtx.val, err = NullsafeAdd(evalCtx.val, v, aggr.fieldType, aggr.prec)
		} else {
			evalCtx.count++
		}
	case AggrTypeAvg:
		if !aggr.isPushDown {
			evalCtx.count++
			evalCtx.val, err = NullsafeSum(evalCtx.val, v, aggr.fieldType, aggr.prec)
		}
	}
	if err != nil {
		evalCtx.hasErr = true
	}
}

// GetResult used to get Value finally.
func (aggr *Aggregation) GetResult(evalCtx *AggEvaluateContext) Value {
	var val Value
	var err error
	if evalCtx.hasErr {
		return MakeTrusted(aggr.fieldType, []byte("0"))
	}
	switch aggr.aggrTyp {
	case AggrTypeAvg:
		if !aggr.isPushDown {
			val, err = NullsafeDiv(evalCtx.val, NewInt64(evalCtx.count), aggr.fieldType, aggr.prec)
		}
	case AggrTypeMax, AggrTypeMin:
		val = evalCtx.val
	case AggrTypeSum:
		val, err = Cast(evalCtx.val, aggr.fieldType)
	case AggrTypeCount:
		if aggr.isPushDown {
			val = evalCtx.val
		} else {
			val = NewInt64(evalCtx.count)
		}
	}
	if err != nil {
		val = MakeTrusted(aggr.fieldType, []byte("0"))
	}
	return val
}

// NewAggEvalCtxs new evalCtxs.
func NewAggEvalCtxs(aggrs []*Aggregation, x []Value) []*AggEvaluateContext {
	var evalCtxs []*AggEvaluateContext
	for _, aggr := range aggrs {
		evalCtx := aggr.InitEvalCtx(x)
		evalCtxs = append(evalCtxs, evalCtx)
	}
	return evalCtxs
}

// GetResults will be called when all data have been processed.
func GetResults(aggrs []*Aggregation, evalCtxs []*AggEvaluateContext, x []Value) ([]Value, []int) {
	var deIdxs []int
	i := 0
	for i < len(aggrs) {
		aggr := aggrs[i]
		evalCtx := evalCtxs[i]
		if aggr.isPushDown && aggr.aggrTyp == AggrTypeAvg {
			var err error
			if x[aggr.index], err = NullsafeDiv(evalCtxs[i+1].val, evalCtxs[i+2].val, aggr.fieldType, aggr.prec); err != nil {
				x[aggr.index] = MakeTrusted(aggr.fieldType, []byte("0"))
			}
			deIdxs = append(deIdxs, aggr.index+1)
			i = i + 2
		} else {
			x[aggr.index] = aggr.GetResult(evalCtx)
		}
		i++
	}

	return x, deIdxs
}
