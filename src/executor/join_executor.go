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
	"sync"
	"xcontext"

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
func (j *JoinExecutor) execute(reqCtx *xcontext.RequestContext, ctx *xcontext.ResultContext) error {
	var mu sync.Mutex
	var wg sync.WaitGroup
	allErrors := make([]error, 0, 8)
	oneExec := func(exec PlanExecutor, ctx *xcontext.ResultContext) {
		defer wg.Done()
		req := xcontext.NewRequestContext()
		req.Mode = reqCtx.Mode
		req.TxnMode = reqCtx.TxnMode
		req.RawQuery = reqCtx.RawQuery

		if err := exec.execute(req, ctx); err != nil {
			mu.Lock()
			allErrors = append(allErrors, err)
			mu.Unlock()
		}
	}

	lctx := xcontext.NewResultContext()
	rctx := xcontext.NewResultContext()
	wg.Add(1)
	go oneExec(j.left, lctx)
	wg.Add(1)
	go oneExec(j.right, rctx)
	wg.Wait()
	if len(allErrors) > 0 {
		return allErrors[0]
	}

	ctx.Results = &sqltypes.Result{}
	ctx.Results.Fields = joinFields(lctx.Results.Fields, rctx.Results.Fields, j.node.Cols)
	if len(lctx.Results.Rows) == 0 {
		return nil
	}

	if len(rctx.Results.Rows) == 0 {
		if !j.node.IsLeftJoin || j.node.HasRightFilter {
			return nil
		}
		for _, lrow := range lctx.Results.Rows {
			ctx.Results.Rows = append(ctx.Results.Rows, joinRows(lrow, nil, j.node.Cols))
			ctx.Results.RowsAffected++
		}
	} else {
		if len(j.node.JoinOn) > 0 {
			if !j.node.IsLeftJoin && (len(lctx.Results.Rows) < RowNumLimit || len(rctx.Results.Rows) < RowNumLimit) {
				simpleBNJoin(lctx.Results.Rows, rctx.Results.Rows, ctx.Results, j.node)
			} else {
				sortMergeJoin(lctx.Results, rctx.Results, ctx.Results, j.node)
			}
		} else {
			for _, lrow := range lctx.Results.Rows {
				for _, rrow := range rctx.Results.Rows {
					ctx.Results.Rows = append(ctx.Results.Rows, joinRows(lrow, rrow, j.node.Cols))
					ctx.Results.RowsAffected++
				}
			}
		}
	}

	return execSubPlan(j.log, j.node, ctx)
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
