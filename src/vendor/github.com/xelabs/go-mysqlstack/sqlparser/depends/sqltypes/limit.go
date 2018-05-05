// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//
// Copyright (c) XeLabs
// BohuTANG

package sqltypes

// Limit used to cutoff the rows based on the MySQL LIMIT and OFFSET clauses.
func (result *Result) Limit(offset, limit int) {
	count := len(result.Rows)
	start := offset
	end := offset + limit
	if start > count {
		start = count
	}
	if end > count {
		end = count
	}
	result.Rows = result.Rows[start:end]
}
