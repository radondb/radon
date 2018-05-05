// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import ()

func (*Explain) iStatement() {}

// Explain represents a explain statement.
type Explain struct {
}

// Format formats the node.
func (node *Explain) Format(buf *TrackedBuffer) {
	buf.WriteString("explain")
}

// WalkSubtree walks the nodes of the subtree.
func (node *Explain) WalkSubtree(visit Visit) error {
	return nil
}
