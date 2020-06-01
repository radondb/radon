/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package engine

import (
	"errors"

	"backend"
	"executor/engine/operator"
	"planner/builder"
	"xcontext"

	"github.com/golang/sync/errgroup"
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
	node        *builder.UnionNode
	left, right PlanEngine
	txn         backend.Transaction
}

// NewUnionEngine creates the new union executor.
func NewUnionEngine(log *xlog.Log, node *builder.UnionNode, txn backend.Transaction) *UnionEngine {
	return &UnionEngine{
		log:  log,
		node: node,
		txn:  txn,
	}
}

// Execute used to execute the executor.
func (u *UnionEngine) Execute(ctx *xcontext.ResultContext) error {
	var eg errgroup.Group

	lctx := xcontext.NewResultContext()
	rctx := xcontext.NewResultContext()

	eg.Go(func() error {
		return u.left.Execute(lctx)
	})
	eg.Go(func() error {
		return u.right.Execute(rctx)
	})
	if err := eg.Wait(); err != nil {
		return err
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
	return operator.ExecSubPlan(u.log, u.node, ctx)
}

// execBindVars used to execute querys with bindvas.
func (u *UnionEngine) execBindVars(ctx *xcontext.ResultContext, bindVars map[string]*querypb.BindVariable, wantfields bool) error {
	return errors.New("UnionEngine.execBindVars: unreachable")
}

// getFields fetches the field info.
func (u *UnionEngine) getFields(ctx *xcontext.ResultContext, bindVars map[string]*querypb.BindVariable) error {
	return errors.New("UnionEngine.getFields: unreachable")
}
