// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//
// Copyright (c) XeLabs
// BohuTANG

package sqltypes

import (
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

// RemoveColumns used to remove columns who in the idxs.
func (result *Result) RemoveColumns(idxs ...int) {
	c := len(idxs)
	if c == 0 {
		return
	}

	if result.Fields != nil {
		var fields []*querypb.Field
		for i, f := range result.Fields {
			in := false
			for _, idx := range idxs {
				if i == idx {
					in = true
					break
				}
			}
			if !in {
				fields = append(fields, f)
			}
		}
		result.Fields = fields
	}

	if result.Rows != nil {
		for i, r := range result.Rows {
			var row []Value
			for i, v := range r {
				in := false
				for _, idx := range idxs {
					if i == idx {
						in = true
						break
					}
				}
				if !in {
					row = append(row, v)
				}
			}
			result.Rows[i] = row
		}
	}
}
