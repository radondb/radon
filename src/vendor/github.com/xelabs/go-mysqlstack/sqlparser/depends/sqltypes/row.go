// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//
// Copyright (c) XeLabs
// BohuTANG

package sqltypes

// Row operations.
type Row []Value

// Copy used to clone the new value.
func (r Row) Copy() []Value {
	ret := make([]Value, len(r))
	for i, v := range r {
		ret[i] = v
	}
	return ret
}
