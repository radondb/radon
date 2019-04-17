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

	"github.com/xelabs/go-mysqlstack/sqlparser"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ PlanExecutor = &MergeExecutor{}
)

// MergeExecutor represents merge executor.
type MergeExecutor struct {
	log  *xlog.Log
	node *planner.MergeNode
	txn  backend.Transaction
}

// NewMergeExecutor creates the new merge executor.
func NewMergeExecutor(log *xlog.Log, node *planner.MergeNode, txn backend.Transaction) *MergeExecutor {
	return &MergeExecutor{
		log:  log,
		node: node,
		txn:  txn,
	}
}

// execute used to execute the executor.
func (m *MergeExecutor) execute(reqCtx *xcontext.RequestContext, ctx *xcontext.ResultContext) error {
	var err error
	reqCtx.Querys = m.node.Querys
	if ctx.Results, err = m.txn.Execute(reqCtx); err != nil {
		return err
	}

	return execSubPlan(m.log, m.node, ctx)
}

// execBindVars used to execute querys with bindvas.
func (m *MergeExecutor) execBindVars(reqCtx *xcontext.RequestContext, ctx *xcontext.ResultContext, bindVars map[string]*querypb.BindVariable, wantfields bool) error {
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

	reqCtx.Querys = querys
	if ctx.Results, err = m.txn.Execute(reqCtx); err != nil {
		return err
	}
	return execSubPlan(m.log, m.node, ctx)
}

// getFields fetches the field info.
func (m *MergeExecutor) getFields(reqCtx *xcontext.RequestContext, ctx *xcontext.ResultContext, bindVars map[string]*querypb.BindVariable) error {
	var err error
	query := m.node.Querys[len(m.node.Querys)-1]
	buf := sqlparser.NewTrackedBuffer(nil)
	sqlparser.FormatImpossibleQuery(buf, m.node.Sel)
	query.Query = buf.String()
	reqCtx.Querys = []xcontext.QueryTuple{query}
	if ctx.Results, err = m.txn.Execute(reqCtx); err != nil {
		return err
	}
	return nil
}
