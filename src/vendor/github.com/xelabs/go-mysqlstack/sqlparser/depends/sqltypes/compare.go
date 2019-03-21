// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

import (
	"bytes"
	"fmt"
)

// Compare used to compare two values.
// if return 0, v1=v2;if -1 v1<v2;if 0 v1>v2.
func Compare(v1, v2 Value) int {
	switch v1.Type() {
	case Decimal:
		v1 = MakeTrusted(Float64, v1.Raw())
	}
	switch v2.Type() {
	case Decimal:
		v2 = MakeTrusted(Float64, v2.Raw())
	}

	if v1.Type() != v2.Type() || v1.IsNull() || v2.IsNull() {
		return -1
	}
	vn1 := v1.ToNative()
	vn2 := v2.ToNative()
	switch vn1.(type) {
	case int64:
		if vn1.(int64) > vn2.(int64) {
			return 1
		}
		if vn1.(int64) < vn2.(int64) {
			return -1
		}
		return 0
	case uint64:
		if vn1.(uint64) > vn2.(uint64) {
			return 1
		}
		if vn1.(uint64) < vn2.(uint64) {
			return -1
		}
		return 0
	case float64:
		if vn1.(float64) > vn2.(float64) {
			return 1
		}
		if vn1.(float64) < vn2.(float64) {
			return -1
		}
		return 0
	case []byte:
		return bytes.Compare(vn1.([]byte), vn2.([]byte))
	default:
		panic(fmt.Sprintf("unsupported.value.type:%T", vn1))
	}
}
