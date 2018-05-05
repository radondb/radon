// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//
// Copyright (c) XeLabs
// BohuTANG

package sqltypes

import (
	"fmt"
	"reflect"
	"testing"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

func TestColumnRemove(t *testing.T) {
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

	{
		rs := rt.Copy()
		rs.RemoveColumns(0)
		{
			want := []*querypb.Field{
				{
					Name: "b",
					Type: Uint24,
				}, {
					Name: "c",
					Type: Float32,
				},
			}
			got := rs.Fields
			if !reflect.DeepEqual(want, got) {
				t.Errorf("want:%+v\n, got:%+v", want, got)
			}
		}

		{
			want := "[[10 3.1415926] [9 3.1415927] [8 3.1415928] [1 3.1415926] [1 3.1415925]]"
			got := fmt.Sprintf("%+v", rs.Rows)
			if want != got {
				t.Errorf("want:%s\n, got:%+s", want, got)
			}
		}
	}

	{
		rs := rt.Copy()
		rs.RemoveColumns(2)
		{
			want := []*querypb.Field{
				{
					Name: "a",
					Type: Int32,
				}, {
					Name: "b",
					Type: Uint24,
				},
			}
			got := rs.Fields
			if !reflect.DeepEqual(want, got) {
				t.Errorf("want:%+v\n, got:%+v", want, got)
			}
		}

		{
			want := "[[-5 10] [-4 9] [-3 8] [1 1] [1 1]]"
			got := fmt.Sprintf("%+v", rs.Rows)
			if want != got {
				t.Errorf("want:%s\n, got:%s", want, got)
			}
		}
	}

	{
		rs := rt.Copy()
		rs.RemoveColumns(0, 1)
		{
			want := []*querypb.Field{
				{
					Name: "c",
					Type: Float32,
				},
			}
			got := rs.Fields
			if !reflect.DeepEqual(want, got) {
				t.Errorf("want:%+v\n, got:%+v", want, got)
			}
		}

		{
			want := "[[3.1415926] [3.1415927] [3.1415928] [3.1415926] [3.1415925]]"
			got := fmt.Sprintf("%+v", rs.Rows)
			if want != got {
				t.Errorf("want:%s\n, got:%s", want, got)
			}
		}
	}
}
