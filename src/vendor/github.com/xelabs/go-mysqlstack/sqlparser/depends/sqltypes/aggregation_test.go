/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package sqltypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

func TestNewAggregation(t *testing.T) {
	plan1 := []struct {
		field    string
		index    int
		typ      AggrType
		distinct bool
	}{{"a", 0, AggrTypeAvg, false}, {"b", 1, AggrTypeSum, false}}

	fields := [][]*querypb.Field{
		{{
			Name: "a",
			Type: querypb.Type_INT32,
		}, {
			Name: "b",
			Type: querypb.Type_INT64,
		}},
		{{
			Name: "a",
			Type: querypb.Type_UINT64,
		}, {
			Name: "b",
			Type: querypb.Type_UINT32,
		}},
		{{
			Name:     "a",
			Type:     Decimal,
			Decimals: 28,
		}, {
			Name:     "b",
			Type:     Decimal,
			Decimals: 28,
		}},
		{{
			Name:     "a",
			Type:     querypb.Type_FLOAT32,
			Decimals: 30,
		}, {
			Name:     "b",
			Type:     querypb.Type_FLOAT64,
			Decimals: 31,
		}},
		{{
			Name: "a",
			Type: querypb.Type_VARCHAR,
		}, {
			Name: "b",
			Type: querypb.Type_YEAR,
		}},
		{{
			Name: "a",
			Type: querypb.Type_YEAR,
		}, {
			Name:     "b",
			Type:     querypb.Type_FLOAT64,
			Decimals: 15,
		}},
		{{
			Name:     "a",
			Type:     querypb.Type_FLOAT32,
			Decimals: 15,
		}, {
			Name: "b",
			Type: querypb.Type_VARCHAR,
		}},
		{{
			Name:     "a",
			Type:     querypb.Type_TIMESTAMP,
			Decimals: 6,
		}, {
			Name: "b",
			Type: querypb.Type_DATETIME,
		}},
		{{
			Name:     "a",
			Type:     querypb.Type_DATE,
			Decimals: 0,
		}, {
			Name: "b",
			Type: querypb.Type_TIME,
		}},
	}

	typs := [][]querypb.Type{
		{Decimal, Decimal},
		{Decimal, Decimal},
		{Decimal, Decimal},
		{querypb.Type_FLOAT64, querypb.Type_FLOAT64},
		{querypb.Type_FLOAT64, Decimal},
		{Decimal, querypb.Type_FLOAT64},
		{querypb.Type_FLOAT64, querypb.Type_FLOAT64},
		{Decimal, Decimal},
		{Decimal, Decimal},
	}
	for i, field := range fields {
		var aggrs []*Aggregation
		for _, plan := range plan1 {
			aggr := NewAggregation(plan.index, plan.typ, plan.distinct, false)
			aggr.FixField(field[aggr.index])
			aggrs = append(aggrs, aggr)
		}

		for j, aggr := range aggrs {
			assert.Equal(t, typs[i][j], aggr.fieldType)
		}
	}

	plan2 := []struct {
		field    string
		index    int
		typ      AggrType
		distinct bool
	}{{"c", 0, AggrTypeCount, false}, {"d", 0, AggrTypeAvg, false}}
	field2 := []*querypb.Field{{
		Name: "c",
		Type: querypb.Type_VARCHAR,
	}}

	aggr2 := NewAggregation(plan2[0].index, plan2[0].typ, plan2[0].distinct, false)
	aggr2.FixField(field2[aggr2.index])
	assert.Equal(t, querypb.Type_INT64, aggr2.fieldType)

	field3 := []*querypb.Field{{
		Name: "d",
		Type: querypb.Type_VARCHAR,
	}}
	aggr3 := NewAggregation(plan2[1].index, plan2[1].typ, plan2[1].distinct, false)
	aggr3.FixField(field3[aggr3.index])
	assert.Equal(t, querypb.Type_FLOAT64, aggr3.fieldType)
}

func TestGetResults(t *testing.T) {
	aggrs := []*Aggregation{
		{
			distinct:   false,
			index:      0,
			aggrTyp:    AggrTypeAvg,
			fieldType:  querypb.Type_FLOAT64,
			isPushDown: true,
			prec:       -1,
		},
		{
			distinct:   false,
			index:      0,
			aggrTyp:    AggrTypeSum,
			fieldType:  querypb.Type_FLOAT64,
			isPushDown: true,
			prec:       -1,
		},
		{
			distinct:   false,
			index:      1,
			aggrTyp:    AggrTypeCount,
			fieldType:  querypb.Type_INT64,
			isPushDown: true,
			prec:       0,
		},
		{
			distinct:   true,
			index:      2,
			aggrTyp:    AggrTypeAvg,
			fieldType:  Decimal,
			isPushDown: false,
			prec:       4,
		},
		{
			distinct:   false,
			index:      3,
			aggrTyp:    AggrTypeCount,
			fieldType:  querypb.Type_INT64,
			isPushDown: false,
			prec:       0,
		},
		{
			distinct:   false,
			index:      4,
			aggrTyp:    AggrTypeMax,
			fieldType:  Decimal,
			isPushDown: false,
			prec:       4,
		},
		{
			distinct:   true,
			index:      5,
			aggrTyp:    AggrTypeMin,
			fieldType:  Decimal,
			isPushDown: true,
			prec:       4,
		},
		{
			distinct:   false,
			index:      6,
			aggrTyp:    AggrTypeSum,
			fieldType:  querypb.Type_FLOAT64,
			isPushDown: false,
			prec:       -1,
		},
		{
			distinct:   false,
			index:      7,
			aggrTyp:    AggrTypeCount,
			fieldType:  querypb.Type_INT64,
			isPushDown: true,
			prec:       0,
		},
	}

	r1 := []Value{
		NewFloat64(3.1),
		NewInt64(2),
		NewInt64(5),
		NewFloat64(3.1),
		MakeTrusted(Decimal, []byte("3.124")),
		MakeTrusted(Decimal, []byte("3.125")),
		MakeTrusted(querypb.Type_VARCHAR, []byte("1nice name")),
		NewInt64(2),
	}

	r2 := []Value{
		NewFloat64(3.5),
		NewInt64(3),
		NewInt64(4),
		NewFloat64(3.5),
		MakeTrusted(Decimal, []byte("3.2")),
		MakeTrusted(Decimal, []byte("3.121")),
		NULL,
		NewInt64(3),
	}

	update := []*AggEvaluateContext{
		{
			count: 0,
			val:   NewFloat64(3.1),
		},
		{
			count: 1,
			val:   NewFloat64(6.6),
		},
		{
			count: 0,
			val:   NewInt64(5),
		},
		{
			count: 2,
			val:   MakeTrusted(Decimal, []byte("9.0000")),
		},
		{
			count: 2,
			val:   NewFloat64(3.1),
		},
		{
			count: 1,
			val:   MakeTrusted(Decimal, []byte("3.2")),
		},
		{
			count: 0,
			val:   MakeTrusted(Decimal, []byte("3.121")),
		},
		{
			count: 1,
			val:   MakeTrusted(querypb.Type_VARCHAR, []byte("1nice name")),
		},
		{
			count: 0,
			val:   NewInt64(5),
		},
	}
	evalCtxs := NewAggEvalCtxs(aggrs, r1)

	for i, aggr := range aggrs {
		aggr.Update(r2, evalCtxs[i])
		assert.Equal(t, update[i].count, evalCtxs[i].count)
		assert.Equal(t, update[i].val, evalCtxs[i].val)
	}

	res := []Value{
		NewFloat64(1.3199999999999998),
		NULL,
		MakeTrusted(Decimal, []byte("4.5000")),
		NewInt64(2),
		MakeTrusted(Decimal, []byte("3.2")),
		MakeTrusted(Decimal, []byte("3.121")),
		NewFloat64(0),
		NewInt64(5),
		NULL,
	}
	x := make([]Value, len(aggrs))
	got, deIdxs := GetResults(aggrs, evalCtxs, x)
	assert.Equal(t, res, got)
	assert.Equal(t, []int{1}, deIdxs)
}
