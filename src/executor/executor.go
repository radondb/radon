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
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// Executor interface.
type Executor interface {
	Execute(*xcontext.ResultContext) error
}

// Tree is a container for all executors
type Tree struct {
	log      *xlog.Log
	children []Executor
	txn      backend.Transaction
	planTree *planner.PlanTree
}

// NewTree creates the new execute tree.
func NewTree(log *xlog.Log, planTree *planner.PlanTree, txn backend.Transaction) *Tree {
	return &Tree{
		log:      log,
		txn:      txn,
		planTree: planTree,
		children: make([]Executor, 0, 16),
	}
}

// Add adds a executor to the tree
func (et *Tree) Add(executor Executor) error {
	et.children = append(et.children, executor)
	return nil
}

// Execute executes all Executor.Execute
func (et *Tree) Execute() (*sqltypes.Result, error) {
	// build tree
	for _, plan := range et.planTree.Plans() {
		switch plan.Type() {
		case planner.PlanTypeDDL:
			executor := NewDDLExecutor(et.log, plan, et.txn)
			if err := et.Add(executor); err != nil {
				return nil, err
			}
		case planner.PlanTypeInsert:
			executor := NewInsertExecutor(et.log, plan, et.txn)
			if err := et.Add(executor); err != nil {
				return nil, err
			}
		case planner.PlanTypeDelete:
			executor := NewDeleteExecutor(et.log, plan, et.txn)
			if err := et.Add(executor); err != nil {
				return nil, err
			}
		case planner.PlanTypeUpdate:
			executor := NewUpdateExecutor(et.log, plan, et.txn)
			if err := et.Add(executor); err != nil {
				return nil, err
			}
		case planner.PlanTypeSelect:
			executor := NewSelectExecutor(et.log, plan, et.txn)
			if err := et.Add(executor); err != nil {
				return nil, err
			}
		case planner.PlanTypeUnion:
			executor := NewUnionExecutor(et.log, plan, et.txn)
			if err := et.Add(executor); err != nil {
				return nil, err
			}
		case planner.PlanTypeOthers:
			executor := NewOthersExecutor(et.log, plan, et.txn)
			if err := et.Add(executor); err != nil {
				return nil, err
			}
		default:
			return nil, errors.Errorf("unsupported.execute.type:%v", plan.Type())
		}
	}

	// execute all
	rsCtx := xcontext.NewResultContext()
	for _, executor := range et.children {
		if err := executor.Execute(rsCtx); err != nil {
			return nil, err
		}
	}
	return rsCtx.Results, nil
}
