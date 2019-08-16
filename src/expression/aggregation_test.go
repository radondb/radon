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
	"testing"

	"github.com/stretchr/testify/assert"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

func TestNewAggregations(t *testing.T) {
	plan1 := []planner.Aggregator{{
		Field:    "a",
		Index:    0,
		Type:     planner.AggrTypeAvg,
		Distinct: false,
	}, {
		Field:    "b",
		Index:    1,
		Type:     planner.AggrTypeSum,
		Distinct: false,
	}}

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
			Type:     sqltypes.Decimal,
			Decimals: 28,
		}, {
			Name:     "b",
			Type:     sqltypes.Decimal,
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
		{sqltypes.Decimal, sqltypes.Decimal},
		{sqltypes.Decimal, sqltypes.Decimal},
		{sqltypes.Decimal, sqltypes.Decimal},
		{querypb.Type_FLOAT64, querypb.Type_FLOAT64},
		{querypb.Type_FLOAT64, sqltypes.Decimal},
		{sqltypes.Decimal, querypb.Type_FLOAT64},
		{querypb.Type_FLOAT64, querypb.Type_FLOAT64},
		{sqltypes.Decimal, sqltypes.Decimal},
		{sqltypes.Decimal, sqltypes.Decimal},
	}
	for i, field := range fields {
		aggrs := NewAggregations(plan1, false, field)
		for j, aggr := range aggrs {
			assert.Equal(t, typs[i][j], aggr.fieldType)
		}
	}

	plan2 := []planner.Aggregator{{
		Field:    "c",
		Index:    0,
		Type:     planner.AggrTypeCount,
		Distinct: false,
	}}
	field2 := []*querypb.Field{{
		Name: "c",
		Type: querypb.Type_VARCHAR,
	}}

	aggr2 := NewAggregations(plan2, false, field2)
	assert.Equal(t, querypb.Type_INT64, aggr2[0].fieldType)
	plan3 := []planner.Aggregator{{
		Field:    "d",
		Index:    0,
		Type:     planner.AggrTypeAvg,
		Distinct: false,
	}}
	field3 := []*querypb.Field{{
		Name: "d",
		Type: querypb.Type_VARCHAR,
	}}
	aggr3 := NewAggregations(plan3, true, field3)
	assert.Equal(t, querypb.Type_FLOAT64, aggr3[0].fieldType)
}

func TestGetResults(t *testing.T) {
	aggrs := []*Aggregation{
		{
			distinct:   false,
			index:      0,
			aggrTyp:    planner.AggrTypeAvg,
			fieldType:  querypb.Type_FLOAT64,
			isPushDown: true,
			prec:       -1,
		},
		{
			distinct:   false,
			index:      0,
			aggrTyp:    planner.AggrTypeSum,
			fieldType:  querypb.Type_FLOAT64,
			isPushDown: true,
			prec:       -1,
		},
		{
			distinct:   false,
			index:      1,
			aggrTyp:    planner.AggrTypeCount,
			fieldType:  querypb.Type_INT64,
			isPushDown: true,
			prec:       0,
		},
		{
			distinct:   true,
			index:      2,
			aggrTyp:    planner.AggrTypeAvg,
			fieldType:  sqltypes.Decimal,
			isPushDown: false,
			prec:       4,
		},
		{
			distinct:   false,
			index:      3,
			aggrTyp:    planner.AggrTypeCount,
			fieldType:  querypb.Type_INT64,
			isPushDown: false,
			prec:       0,
		},
		{
			distinct:   false,
			index:      4,
			aggrTyp:    planner.AggrTypeMax,
			fieldType:  sqltypes.Decimal,
			isPushDown: false,
			prec:       4,
		},
		{
			distinct:   true,
			index:      5,
			aggrTyp:    planner.AggrTypeMin,
			fieldType:  sqltypes.Decimal,
			isPushDown: true,
			prec:       4,
		},
		{
			distinct:   false,
			index:      6,
			aggrTyp:    planner.AggrTypeSum,
			fieldType:  querypb.Type_FLOAT64,
			isPushDown: false,
			prec:       -1,
		},
		{
			distinct:   false,
			index:      7,
			aggrTyp:    planner.AggrTypeCount,
			fieldType:  querypb.Type_INT64,
			isPushDown: true,
			prec:       0,
		},
	}

	r1 := []sqltypes.Value{
		sqltypes.NewFloat64(3.1),
		sqltypes.NewInt64(2),
		sqltypes.NewInt64(5),
		sqltypes.NewFloat64(3.1),
		sqltypes.MakeTrusted(sqltypes.Decimal, []byte("3.124")),
		sqltypes.MakeTrusted(sqltypes.Decimal, []byte("3.125")),
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("1nice name")),
		sqltypes.NewInt64(2),
	}

	r2 := []sqltypes.Value{
		sqltypes.NewFloat64(3.5),
		sqltypes.NewInt64(3),
		sqltypes.NewInt64(4),
		sqltypes.NewFloat64(3.5),
		sqltypes.MakeTrusted(sqltypes.Decimal, []byte("3.2")),
		sqltypes.MakeTrusted(sqltypes.Decimal, []byte("3.121")),
		sqltypes.NULL,
		sqltypes.NewInt64(3),
	}

	update := []*AggEvaluateContext{
		{
			count: 0,
			val:   sqltypes.NewFloat64(3.1),
		},
		{
			count: 1,
			val:   sqltypes.NewFloat64(6.6),
		},
		{
			count: 0,
			val:   sqltypes.NewInt64(5),
		},
		{
			count: 2,
			val:   sqltypes.MakeTrusted(sqltypes.Decimal, []byte("9")),
		},
		{
			count: 2,
			val:   sqltypes.NewFloat64(3.1),
		},
		{
			count: 1,
			val:   sqltypes.MakeTrusted(sqltypes.Decimal, []byte("3.2")),
		},
		{
			count: 0,
			val:   sqltypes.MakeTrusted(sqltypes.Decimal, []byte("3.121")),
		},
		{
			count: 1,
			val:   sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("1nice name")),
		},
		{
			count: 0,
			val:   sqltypes.NewInt64(5),
		},
	}
	evalCtxs := NewAggEvalCtxs(aggrs, r1)

	for i, aggr := range aggrs {
		aggr.Update(r2, evalCtxs[i])
		assert.Equal(t, update[i].count, evalCtxs[i].count)
		assert.Equal(t, update[i].val, evalCtxs[i].val)
	}

	res := []sqltypes.Value{
		sqltypes.NewFloat64(1.3199999999999998),
		sqltypes.NULL,
		sqltypes.MakeTrusted(sqltypes.Decimal, []byte("4.5000")),
		sqltypes.NewInt64(2),
		sqltypes.MakeTrusted(sqltypes.Decimal, []byte("3.2")),
		sqltypes.MakeTrusted(sqltypes.Decimal, []byte("3.121")),
		sqltypes.NewFloat64(0),
		sqltypes.NewInt64(5),
		sqltypes.NULL,
	}
	x := make([]sqltypes.Value, len(aggrs))
	got, deIdxs := GetResults(aggrs, evalCtxs, x)
	assert.Equal(t, res, got)
	assert.Equal(t, []int{1}, deIdxs)
}
