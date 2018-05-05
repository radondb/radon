// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqlparser

import ()

const (
	// StartTxnStr represents the txn start.
	StartTxnStr = "start transaction"

	// CommitTxnStr represents the txn commit.
	CommitTxnStr = "commit"
)

// Transaction represents the transaction tuple.
type Transaction struct {
	Action string
}

func (*Transaction) iStatement() {}

// Format formats the node.
func (node *Transaction) Format(buf *TrackedBuffer) {
	switch node.Action {
	case StartTxnStr:
		buf.WriteString(StartTxnStr)
	case CommitTxnStr:
		buf.WriteString(CommitTxnStr)
	}
}

// WalkSubtree walks the nodes of the subtree.
func (node *Transaction) WalkSubtree(visit Visit) error {
	return nil
}
