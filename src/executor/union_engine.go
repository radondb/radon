/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package executor

import (
	"errors"
	"sync"

	"backend"
	"planner"
	"xcontext"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ PlanEngine = &UnionEngine{}
)

// UnionEngine represents merge executor.
type UnionEngine struct {
	log         *xlog.Log
	node        *planner.UnionNode
	left, right PlanEngine
	txn         backend.Transaction
}

// NewUnionEngine creates the new union executor.
func NewUnionEngine(log *xlog.Log, node *planner.UnionNode, txn backend.Transaction) *UnionEngine {
	return &UnionEngine{
		log:  log,
		node: node,
		txn:  txn,
	}
}

// execute used to execute the executor.
func (u *UnionEngine) execute(ctx *xcontext.ResultContext) error {
	var mu sync.Mutex
	var wg sync.WaitGroup
	allErrors := make([]error, 0, 8)
	oneExec := func(exec PlanEngine, ctx *xcontext.ResultContext) {
		defer wg.Done()
		if err := exec.execute(ctx); err != nil {
			mu.Lock()
			allErrors = append(allErrors, err)
			mu.Unlock()
		}
	}
	lctx := xcontext.NewResultContext()
	rctx := xcontext.NewResultContext()
	wg.Add(1)
	go oneExec(u.left, lctx)
	wg.Add(1)
	go oneExec(u.right, rctx)
	wg.Wait()
	if len(allErrors) > 0 {
		return allErrors[0]
	}

	if len(lctx.Results.Fields) != len(rctx.Results.Fields) {
		return errors.New("unsupported: the.used.'select'.statements.have.a.different.number.of.columns")
	}
	ctx.Results = &sqltypes.Result{}
	ctx.Results.Fields = lctx.Results.Fields
	lctx.Results.AppendResult(rctx.Results)
	if len(lctx.Results.Rows) == 0 {
		return nil
	}
	if u.node.Typ == "union distinct" || u.node.Typ == "union" {
		table := common.NewHashTable()
		for _, row := range lctx.Results.Rows {
			var key []byte
			for _, v := range row {
				key = append(key, v.Raw()...)
			}
			if has, _ := table.Get(key); !has {
				table.Put(key, row)
			}
		}
		for _, v, next := table.Next()(); next != nil; _, v, next = next() {
			ctx.Results.Rows = append(ctx.Results.Rows, v[0].([]sqltypes.Value))
		}
		ctx.Results.RowsAffected = uint64(table.Size())
	} else {
		ctx.Results.Rows = lctx.Results.Rows
		ctx.Results.RowsAffected = lctx.Results.RowsAffected
	}
	return execSubPlan(u.log, u.node, ctx)
}

// execBindVars used to execute querys with bindvas.
func (u *UnionEngine) execBindVars(ctx *xcontext.ResultContext, bindVars map[string]*querypb.BindVariable, wantfields bool) error {
	return errors.New("UnionEngine.execBindVars: unreachable")
}

// getFields fetches the field info.
func (u *UnionEngine) getFields(ctx *xcontext.ResultContext, bindVars map[string]*querypb.BindVariable) error {
	return errors.New("UnionEngine.getFields: unreachable")
}
