/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package engine

import (
	"backend"
	"executor/engine/operator"
	"planner/builder"
	"xcontext"

	"github.com/xelabs/go-mysqlstack/sqlparser"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ PlanEngine = &MergeEngine{}
)

// MergeEngine represents merge executor.
type MergeEngine struct {
	log  *xlog.Log
	node *builder.MergeNode
	txn  backend.Transaction
}

// NewMergeEngine creates the new merge executor.
func NewMergeEngine(log *xlog.Log, node *builder.MergeNode, txn backend.Transaction) *MergeEngine {
	return &MergeEngine{
		log:  log,
		node: node,
		txn:  txn,
	}
}

// Execute used to execute the executor.
func (m *MergeEngine) Execute(ctx *xcontext.ResultContext) error {
	var err error

	reqCtx := xcontext.NewRequestContext()
	reqCtx.Mode = m.node.ReqMode
	reqCtx.TxnMode = xcontext.TxnRead
	if reqCtx.Mode == xcontext.ReqNormal {
		reqCtx.Querys = m.node.Querys
	} else {
		buf := sqlparser.NewTrackedBuffer(nil)
		m.node.Sel.Format(buf)
		reqCtx.RawQuery = buf.String()
	}

	if ctx.Results, err = m.txn.Execute(reqCtx); err != nil {
		return err
	}
	return operator.ExecSubPlan(m.log, m.node, ctx)
}

// execBindVars used to execute querys with bindvas.
func (m *MergeEngine) execBindVars(ctx *xcontext.ResultContext, bindVars map[string]*querypb.BindVariable, wantfields bool) error {
	var query string
	var err error

	querys := m.node.Querys
	for i, p := range m.node.ParsedQuerys {
		query, err = p.GenerateQuery(bindVars, nil)
		if err != nil {
			return err
		}
		querys[i].Query = query
	}

	reqCtx := xcontext.NewRequestContext()
	reqCtx.Mode = xcontext.ReqNormal
	reqCtx.TxnMode = xcontext.TxnRead
	reqCtx.Querys = querys

	if ctx.Results, err = m.txn.Execute(reqCtx); err != nil {
		return err
	}
	return operator.ExecSubPlan(m.log, m.node, ctx)
}

// getFields fetches the field info.
func (m *MergeEngine) getFields(ctx *xcontext.ResultContext, bindVars map[string]*querypb.BindVariable) error {
	var err error

	query := m.node.Querys[len(m.node.Querys)-1]
	query.Query, err = m.node.GenerateFieldQuery().GenerateQuery(bindVars, nil)
	if err != nil {
		return err
	}

	reqCtx := xcontext.NewRequestContext()
	reqCtx.Mode = xcontext.ReqNormal
	reqCtx.TxnMode = xcontext.TxnRead
	reqCtx.Querys = []xcontext.QueryTuple{query}

	if ctx.Results, err = m.txn.Execute(reqCtx); err != nil {
		return err
	}
	return nil
}
