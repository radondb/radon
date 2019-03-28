// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//
// Copyright (c) XeLabs
// BohuTANG

package sqltypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompare(t *testing.T) {
	// uint64.
	{
		v1 := testVal(Uint24, "3")
		v2 := testVal(Uint24, "5")
		v3 := testVal(Uint24, "4")

		cmp := Compare(v1, v2)
		assert.Equal(t, -1, cmp)

		cmp = Compare(v2, v3)
		assert.Equal(t, 1, cmp)

		cmp = Compare(v1, v1)
		assert.Equal(t, 0, cmp)
	}
	// int64.
	{
		v1 := testVal(Int64, "3")
		v2 := testVal(Int64, "5")
		v3 := testVal(Int64, "4")

		cmp := Compare(v1, v2)
		assert.Equal(t, -1, cmp)

		cmp = Compare(v2, v3)
		assert.Equal(t, 1, cmp)

		cmp = Compare(v1, v1)
		assert.Equal(t, 0, cmp)
	}
	// float64.
	{
		v1 := testVal(Decimal, "3.14159")
		v2 := testVal(Decimal, "3.142")
		v3 := testVal(Decimal, "3.1416")

		cmp := Compare(v1, v2)
		assert.Equal(t, -1, cmp)

		cmp = Compare(v2, v3)
		assert.Equal(t, 1, cmp)

		cmp = Compare(v1, v1)
		assert.Equal(t, 0, cmp)
	}
	// []byte.
	{
		v1 := testVal(VarChar, "pra")
		v2 := testVal(VarChar, "sci")
		v3 := testVal(VarChar, "qq")

		cmp := Compare(v1, v2)
		assert.Equal(t, -1, cmp)

		cmp = Compare(v2, v3)
		assert.Equal(t, 1, cmp)

		cmp = Compare(v1, v1)
		assert.Equal(t, 0, cmp)
	}
	// different type.
	{
		v1 := testVal(Int64, "3")
		v2 := testVal(Decimal, "3.0")
		v3 := testVal(VarChar, "3")

		cmp := Compare(v1, v2)
		assert.Equal(t, 0, cmp)

		cmp = Compare(v1, v3)
		assert.Equal(t, 0, cmp)

		cmp = Compare(v2, v3)
		assert.Equal(t, 0, cmp)
	}
	// different type.
	{
		v1 := testVal(Float64, "2.5")
		v2 := testVal(Decimal, "2.5")

		cmp := Compare(v1, v2)
		assert.Equal(t, 0, cmp)
	}
	// v1 is null.
	{
		v1 := NULL
		v2 := testVal(VarChar, "qq")

		cmp := Compare(v1, v2)
		assert.Equal(t, -1, cmp)
	}
	// v2 is null.
	{
		v1 := testVal(VarChar, "qq")
		v2 := NULL

		cmp := Compare(v1, v2)
		assert.Equal(t, 1, cmp)
	}
	// v1 v2 are null.
	{
		v1 := NULL
		v2 := NULL

		cmp := Compare(v1, v2)
		assert.Equal(t, 0, cmp)
	}

	// uint64.
	{
		v1 := testVal(Uint24, "5")
		v2 := testVal(Float64, "-5.1")
		v3 := testVal(VarChar, "a")

		cmp := Compare(v1, v2)
		assert.Equal(t, 1, cmp)

		cmp = Compare(v2, v3)
		assert.Equal(t, -1, cmp)

		cmp = Compare(v1, v3)
		assert.Equal(t, 1, cmp)
	}
}
