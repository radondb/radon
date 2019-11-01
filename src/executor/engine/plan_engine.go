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
	"planner/builder"
	"xcontext"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// PlanEngine interface.
type PlanEngine interface {
	Execute(ctx *xcontext.ResultContext) error
	execBindVars(ctx *xcontext.ResultContext, bindVars map[string]*querypb.BindVariable, wantfields bool) error
	getFields(ctx *xcontext.ResultContext, bindVars map[string]*querypb.BindVariable) error
}

// BuildEngine used to build the executor tree.
func BuildEngine(log *xlog.Log, plan builder.PlanNode, txn backend.Transaction) PlanEngine {
	var engine PlanEngine
	switch node := plan.(type) {
	case *builder.MergeNode:
		engine = NewMergeEngine(log, node, txn)
	case *builder.JoinNode:
		joinEngine := NewJoinEngine(log, node, txn)
		joinEngine.left = BuildEngine(log, node.Left, txn)
		joinEngine.right = BuildEngine(log, node.Right, txn)
		engine = joinEngine
	case *builder.UnionNode:
		unionEngine := NewUnionEngine(log, node, txn)
		unionEngine.left = BuildEngine(log, node.Left, txn)
		unionEngine.right = BuildEngine(log, node.Right, txn)
		engine = unionEngine
	}
	return engine
}
