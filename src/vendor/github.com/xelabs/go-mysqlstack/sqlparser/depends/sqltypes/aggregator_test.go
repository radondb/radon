// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//
// Copyright (c) XeLabs
// BohuTANG

package sqltypes

import (
	"fmt"
	"testing"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

func testOperator(typ string, x []Value) func([]Value) []Value {
	return func(y []Value) []Value {
		c := len(x)
		ret := Row(x).Copy()
		for i := 0; i < c; i++ {
			switch typ {
			case "sum", "count":
				v1, v2 := x[i], y[i]
				ret[i] = Operator(v1, v2, SumFn)
			case "min":
				v1, v2 := x[i], y[i]
				ret[i] = Operator(v1, v2, MinFn)
			case "max":
				v1, v2 := x[i], y[i]
				ret[i] = Operator(v1, v2, MaxFn)
			}
		}
		return ret
	}
}

func testAggregate(typ string, result *Result) {
	key := "xx"
	groups := make(map[string][]Value)
	for _, row1 := range result.Rows {
		if row2, ok := groups[key]; !ok {
			groups[key] = row1
		} else {
			groups[key] = testOperator(typ, row1)(row2)
		}
	}

	i := 0
	result.Rows = make([][]Value, len(groups))
	for _, v := range groups {
		result.Rows[i] = v
		i++
	}
	result.OrderedByAsc(result.Fields[0].Name)
	result.Sort()
}

func TestAggregator(t *testing.T) {
	rt := &Result{
		Fields: []*querypb.Field{{
			Name: "a",
			Type: Int32,
		}, {
			Name: "b",
			Type: Uint24,
		}, {
			Name: "c",
			Type: Float32,
		},
		},
		Rows: [][]Value{
			{testVal(Int32, "-5"), testVal(Uint64, "10"), testVal(Float32, "3.1415926")},
			{testVal(Int32, "-4"), testVal(Uint64, "9"), testVal(Float32, "3.1415927")},
			{testVal(Int32, "-3"), testVal(Uint64, "8"), testVal(Float32, "3.1415928")},
			{testVal(Int32, "1"), testVal(Uint64, "1"), testVal(Float32, "3.1415926")},
			{testVal(Int32, "1"), testVal(Uint64, "1"), testVal(Float32, "3.1415925")},
		},
	}

	// sum aggregator.
	{
		rs := rt.Copy()
		testAggregate("sum", rs)
		want := "[[-10 29 15.7079632]]"
		got := fmt.Sprintf("%+v", rs.Rows)
		if want != got {
			t.Errorf("want:%s\n, got:%s", want, got)
		}
	}

	// count aggregator.
	{
		rs := rt.Copy()
		testAggregate("count", rs)
		want := "[[-10 29 15.7079632]]"
		got := fmt.Sprintf("%+v", rs.Rows)
		if want != got {
			t.Errorf("want:%s\n, got:%s", want, got)
		}
	}

	// min aggregator.
	{
		rs := rt.Copy()
		testAggregate("min", rs)
		want := "[[-5 1 3.1415925]]"
		got := fmt.Sprintf("%+v", rs.Rows)
		if want != got {
			t.Errorf("want:%s\n, got:%s", want, got)
		}
	}

	// max aggregator.
	{
		rs := rt.Copy()
		testAggregate("max", rs)
		want := "[[1 10 3.1415928]]"
		got := fmt.Sprintf("%+v", rs.Rows)
		if want != got {
			t.Errorf("want:%s\n, got:%s", want, got)
		}
	}

	// div aggregator.
	{
		v1 := testVal(Int32, "7")
		v2 := testVal(Float32, "3.1415926")
		ret := Operator(v1, v2, DivFn)
		want := "2.2281692412950043"
		got := fmt.Sprintf("%+v", ret)
		if want != got {
			t.Errorf("want:%s\n, got:%s", want, got)
		}
	}
}

func TestOperator(t *testing.T) {
	{
		x := testVal(Decimal, "3.1415926")
		y := testVal(Decimal, "3")
		f := Operator(x, y, SumFn)
		got := fmt.Sprintf("%+v", f.Raw())
		want := "[54 46 49 52 49 53 57 50 54]"
		if want != got {
			t.Errorf("want:%s\n, got:%s", want, got)
		}
	}

	{
		x := testVal(Null, "")
		y := testVal(Decimal, "3")
		f := Operator(x, y, SumFn)
		got := fmt.Sprintf("%+v", f.Raw())
		want := "[]"
		if want != got {
			t.Errorf("want:%s\n, got:%s", want, got)
		}
	}
}
