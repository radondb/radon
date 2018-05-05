// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import (
	"strconv"
)

// NumVal represents numval tuple.
type NumVal struct {
	raw string
}

// AsUint64 returns uint64 value.
func (exp *NumVal) AsUint64() uint64 {
	v, err := strconv.ParseUint(exp.raw, 10, 64)
	if err != nil {
		return 1<<63 - 1
	}
	return v
}

func (*Kill) iStatement() {}

// Kill represents a KILL statement.
type Kill struct {
	QueryID *NumVal
}

// Format formats the node.
func (node *Kill) Format(buf *TrackedBuffer) {
	buf.Myprintf("kill %s", node.QueryID.raw)
}

// WalkSubtree walks the nodes of the subtree.
func (node *Kill) WalkSubtree(visit Visit) error {
	return nil
}
