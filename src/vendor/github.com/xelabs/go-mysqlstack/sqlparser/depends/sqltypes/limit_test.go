// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//
// Copyright (c) XeLabs
// BohuTANG

package sqltypes

import (
	"reflect"
	"testing"
)

func TestLimit(t *testing.T) {
	rs := &Result{
		Rows: [][]Value{
			{testVal(VarChar, "1")}, {testVal(VarChar, "2")}, {testVal(VarChar, "3")}, {testVal(VarChar, "4")}, {testVal(VarChar, "5")},
		},
	}

	// normal: offset 0, limit 1.
	{
		rs1 := rs.Copy()
		rs1.Limit(0, 1)
		want := rs.Rows[0:1]
		got := rs1.Rows

		if !reflect.DeepEqual(want, got) {
			t.Errorf("want:\n%#v, got\n%#v", want, got)
		}
	}

	// normal: offset 0, limit 5.
	{
		rs1 := rs.Copy()
		rs1.Limit(0, 5)
		want := rs.Rows
		got := rs1.Rows

		if !reflect.DeepEqual(want, got) {
			t.Errorf("want:\n%#v, got\n%#v", want, got)
		}
	}

	// normal: offset 1, limit 4.
	{
		rs1 := rs.Copy()
		rs1.Limit(1, 4)
		want := rs.Rows[1:5]
		got := rs1.Rows

		if !reflect.DeepEqual(want, got) {
			t.Errorf("want:\n%#v, got\n%#v", want, got)
		}
	}

	// limit overflow: offset 0, limit 6.
	{
		rs1 := rs.Copy()
		rs1.Limit(0, 6)
		want := rs.Rows
		got := rs1.Rows

		if !reflect.DeepEqual(want, got) {
			t.Errorf("want:\n%#v, got\n%#v", want, got)
		}
	}

	// offset overflow: offset 5, limit 0.
	{
		rs1 := rs.Copy()
		rs1.Limit(5, 0)
		want := rs.Rows[5:5]
		got := rs1.Rows

		if !reflect.DeepEqual(want, got) {
			t.Errorf("want:\n%#v, got\n%#v", want, got)
		}
	}

	// (offset+limit) overflow: offset 3, limit 6.
	{
		rs1 := rs.Copy()
		rs1.Limit(3, 6)
		want := rs.Rows[3:5]
		got := rs1.Rows

		if !reflect.DeepEqual(want, got) {
			t.Errorf("want:\n%#v, got\n%#v", want, got)
		}
	}

	// Empty test.
	{
		rs1 := &Result{
			Rows: [][]Value{
				{},
			},
		}

		rs1.Limit(3, 6)
		want := rs.Rows[0:0]
		got := rs1.Rows

		if !reflect.DeepEqual(want, got) {
			t.Errorf("want:\n%#v, got\n%#v", want, got)
		}
	}
}
