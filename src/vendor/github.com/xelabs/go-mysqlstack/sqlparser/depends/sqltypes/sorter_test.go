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

func TestSorter(t *testing.T) {
	rs := &Result{
		Fields: []*querypb.Field{{
			Name: "user",
			Type: VarChar,
		}, {
			Name: "language",
			Type: VarChar,
		}, {
			Name: "lines",
			Type: Int64,
		},
		},
		Rows: [][]Value{
			{testVal(VarChar, "gri"), testVal(VarChar, "Go"), testVal(Int64, "100")},
			{testVal(VarChar, "ken"), testVal(VarChar, "C"), testVal(Int64, "150")},
			{testVal(VarChar, "glenda"), testVal(VarChar, "Go"), testVal(Int64, "200")},
			{testVal(VarChar, "rsc"), testVal(VarChar, "Go"), testVal(Int64, "200")},
			{testVal(VarChar, "r"), testVal(VarChar, "Go"), testVal(Int64, "200")},
			{testVal(VarChar, "ken"), testVal(VarChar, "Go"), testVal(Int64, "200")},
			{testVal(VarChar, "dmr"), testVal(VarChar, "C"), testVal(Int64, "100")},
			{testVal(VarChar, "r"), testVal(VarChar, "C"), testVal(Int64, "150")},
			{testVal(VarChar, "gri"), testVal(VarChar, "Smalltalk"), testVal(Int64, "80")},
		},
	}

	// asc
	{
		fields := []string{
			"user",
			"language",
			"lines",
		}
		wants := []string{
			"[[dmr C 100] [glenda Go 200] [gri Go 100] [gri Smalltalk 80] [ken C 150] [ken Go 200] [r Go 200] [r C 150] [rsc Go 200]]",
			"[[dmr C 100] [ken C 150] [r C 150] [glenda Go 200] [rsc Go 200] [r Go 200] [ken Go 200] [gri Go 100] [gri Smalltalk 80]]",
			"[[gri Smalltalk 80] [gri Go 100] [dmr C 100] [ken C 150] [r C 150] [rsc Go 200] [r Go 200] [ken Go 200] [glenda Go 200]]",
		}

		for i, field := range fields {
			rs1 := rs.Copy()
			rs1.OrderedByAsc(field)
			rs1.Sort()

			want := wants[i]
			got := fmt.Sprintf("%+v", rs1.Rows)
			if want != got {
				t.Errorf("want:%s\n, got:%s", want, got)
			}
		}
	}

	// desc
	{
		fields := []string{
			"user",
			"language",
			"lines",
		}
		wants := []string{
			"[[rsc Go 200] [r C 150] [r Go 200] [ken Go 200] [ken C 150] [gri Go 100] [gri Smalltalk 80] [glenda Go 200] [dmr C 100]]",
			"[[gri Smalltalk 80] [gri Go 100] [rsc Go 200] [r Go 200] [ken Go 200] [glenda Go 200] [ken C 150] [dmr C 100] [r C 150]]",
			"[[glenda Go 200] [rsc Go 200] [r Go 200] [ken Go 200] [ken C 150] [r C 150] [gri Go 100] [dmr C 100] [gri Smalltalk 80]]",
		}

		for i, field := range fields {
			rs1 := rs.Copy()
			rs1.OrderedByDesc(field)
			rs1.Sort()

			want := wants[i]
			got := fmt.Sprintf("%+v", rs1.Rows)
			if want != got {
				t.Errorf("want:%s\n, got:%s", want, got)
			}
		}
	}

	// user + language + lines asc
	{
		fields := []string{
			"user",
			"language",
			"lines",
		}

		rs1 := rs.Copy()
		rs1.OrderedByAsc(fields...)
		rs1.Sort()
		want := "[[dmr C 100] [glenda Go 200] [gri Go 100] [gri Smalltalk 80] [ken C 150] [ken Go 200] [r C 150] [r Go 200] [rsc Go 200]]"
		got := fmt.Sprintf("%+v", rs1.Rows)
		if want != got {
			t.Errorf("want:%s\n, got:%s", want, got)
		}
	}

	// user + language + lines desc
	{
		fields := []string{
			"user",
			"language",
			"lines",
		}

		rs1 := rs.Copy()
		rs1.OrderedByDesc(fields...)
		rs1.Sort()
		want := "[[rsc Go 200] [r Go 200] [r C 150] [ken Go 200] [ken C 150] [gri Smalltalk 80] [gri Go 100] [glenda Go 200] [dmr C 100]]"
		got := fmt.Sprintf("%+v", rs1.Rows)
		if want != got {
			t.Errorf("want:%s\n, got:%s", want, got)
		}
	}
}

func TestSorterType(t *testing.T) {
	rs := &Result{
		Fields: []*querypb.Field{{
			Name: "ID",
			Type: Uint24,
		}, {
			Name: "cost",
			Type: Float32,
		}, {
			Name: "nil",
			Type: Null,
		}},
		Rows: [][]Value{
			{testVal(Uint24, "3"), testVal(Float32, "3.1415926"), NULL},
			{testVal(Uint24, "7"), testVal(Float32, "3.1415926"), NULL},
			{testVal(Uint24, "2"), testVal(Float32, "3.1415927"), NULL},
		},
	}

	// asc
	{
		fields := []string{
			"ID",
			"cost",
			"nil",
		}
		wants := []string{
			"[[2 3.1415927 ] [3 3.1415926 ] [7 3.1415926 ]]",
			"[[3 3.1415926 ] [7 3.1415926 ] [2 3.1415927 ]]",
			"[[3 3.1415926 ] [7 3.1415926 ] [2 3.1415927 ]]",
		}

		for i, field := range fields {
			rs1 := rs.Copy()
			rs1.OrderedByAsc(field)
			rs1.Sort()

			want := wants[i]
			got := fmt.Sprintf("%+v", rs1.Rows)
			if want != got {
				t.Errorf("want:%s\n, got:%s", want, got)
			}
		}
	}

	// desc
	{
		fields := []string{
			"ID",
			"cost",
			"nil",
		}
		wants := []string{
			"[[7 3.1415926 ] [3 3.1415926 ] [2 3.1415927 ]]",
			"[[2 3.1415927 ] [3 3.1415926 ] [7 3.1415926 ]]",
			"[[3 3.1415926 ] [7 3.1415926 ] [2 3.1415927 ]]",
		}

		for i, field := range fields {
			rs1 := rs.Copy()
			rs1.OrderedByDesc(field)
			rs1.Sort()

			want := wants[i]
			got := fmt.Sprintf("%+v", rs1.Rows)
			if want != got {
				t.Errorf("want:%s\n, got:%s", want, got)
			}
		}
	}
}

func TestSorterError(t *testing.T) {
	rs := &Result{
		Fields: []*querypb.Field{{
			Name: "ID",
			Type: Uint24,
		}, {
			Name: "cost",
			Type: Float32,
		},
		},
		Rows: [][]Value{
			{testVal(Uint24, "3"), testVal(Float32, "3.1415926")},
			{testVal(Uint24, "7"), testVal(Float32, "3.1415926")},
			{testVal(Uint24, "2"), testVal(Float32, "3.1415927")},
		},
	}

	// Field error.
	{
		{
			rs1 := rs.Copy()
			err := rs1.OrderedByAsc("xx")
			want := "can.not.find.the.orderby.field[xx].direction.asc"
			got := err.Error()
			if want != got {
				t.Errorf("want:%s\n, got:%s", want, got)
			}
		}

		{
			rs1 := rs.Copy()
			err := rs1.OrderedByDesc("xx")
			want := "can.not.find.the.orderby.field[xx].direction.desc"
			got := err.Error()
			if want != got {
				t.Errorf("want:%s\n, got:%s", want, got)
			}
		}

	}
}
