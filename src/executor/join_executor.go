/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package executor

import (
	"backend"
	"planner"
	"xcontext"

	"github.com/pkg/errors"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ PlanExecutor = &JoinExecutor{}
)

// JoinExecutor represents join executor.
type JoinExecutor struct {
	log         *xlog.Log
	node        *planner.JoinNode
	left, right PlanExecutor
	txn         backend.Transaction
}

// NewJoinExecutor creates the new join executor.
func NewJoinExecutor(log *xlog.Log, node *planner.JoinNode, txn backend.Transaction) *JoinExecutor {
	return &JoinExecutor{
		log:  log,
		node: node,
		txn:  txn,
	}
}

// execute used to execute the executor.
func (m *JoinExecutor) execute(reqCtx *xcontext.RequestContext, ctx *xcontext.ResultContext) error {
	return errors.New("unsupported: join")
}

// joinFields used to join two fields.
func joinFields(lfields, rfields []*querypb.Field, cols []int) []*querypb.Field {
	fields := make([]*querypb.Field, len(cols))
	for i, index := range cols {
		if index < 0 {
			fields[i] = lfields[-index-1]
			continue
		}
		fields[i] = rfields[index-1]
	}
	return fields
}

// joinRows used to join two rows.
func joinRows(lrow, rrow []sqltypes.Value, cols []int) []sqltypes.Value {
	row := make([]sqltypes.Value, len(cols))
	for i, index := range cols {
		if index < 0 {
			row[i] = lrow[-index-1]
			continue
		}
		// rrow can be nil on left joins
		if rrow != nil {
			row[i] = rrow[index-1]
		}
	}
	return row
}
