// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import ()

func (*Xa) iStatement() {}

// Xa represents a XA statement.
type Xa struct {
}

// Format formats the node.
func (node *Xa) Format(buf *TrackedBuffer) {
	buf.WriteString("XA")
}

// WalkSubtree walks the nodes of the subtree.
func (node *Xa) WalkSubtree(visit Visit) error {
	return nil
}
