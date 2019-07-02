/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package executor

import (
	"sync"

	"backend"
	"planner"
	"xcontext"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_ PlanEngine = &JoinEngine{}
)

// JoinEngine represents join executor.
type JoinEngine struct {
	log         *xlog.Log
	node        *planner.JoinNode
	left, right PlanEngine
	txn         backend.Transaction
}

// NewJoinEngine creates the new join executor.
func NewJoinEngine(log *xlog.Log, node *planner.JoinNode, txn backend.Transaction) *JoinEngine {
	return &JoinEngine{
		log:  log,
		node: node,
		txn:  txn,
	}
}

// execute used to execute the executor.
func (j *JoinEngine) execute(ctx *xcontext.ResultContext) error {
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

	if j.node.Strategy == planner.NestedLoop {
		joinVars := make(map[string]*querypb.BindVariable)
		if err := j.execBindVars(ctx, joinVars, true); err != nil {
			return err
		}
	} else {
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
			concatLeftAndNil(lctx.Results.Rows, j.node, ctx.Results)
		} else {
			switch j.node.Strategy {
			case planner.SortMerge:
				sortMergeJoin(lctx.Results, rctx.Results, ctx.Results, j.node)
			case planner.Cartesian:
				cartesianProduct(lctx.Results, rctx.Results, ctx.Results, j.node)
			}
		}
	}

	return execSubPlan(j.log, j.node, ctx)
}

// execBindVars used to execute querys with bindvas.
func (j *JoinEngine) execBindVars(ctx *xcontext.ResultContext, bindVars map[string]*querypb.BindVariable, wantfields bool) error {
	var err error
	lctx := xcontext.NewResultContext()
	rctx := xcontext.NewResultContext()
	ctx.Results = &sqltypes.Result{}

	joinVars := make(map[string]*querypb.BindVariable)
	if err = j.left.execBindVars(lctx, bindVars, wantfields); err != nil {
		return err
	}

	for _, lrow := range lctx.Results.Rows {
		blend := true
		matchCnt := 0
		for _, idx := range j.node.LeftTmpCols {
			vn := lrow[idx].ToNative()
			if vn.(int64) == 0 {
				blend = false
				break
			}
		}
		if blend {
			for k, col := range j.node.Vars {
				joinVars[k] = sqltypes.ValueBindVariable(lrow[col])
			}
			if err = j.right.execBindVars(rctx, combineVars(bindVars, joinVars), wantfields); err != nil {
				return err
			}
			if wantfields {
				wantfields = false
				ctx.Results.Fields = joinFields(lctx.Results.Fields, rctx.Results.Fields, j.node.Cols)
			}
			for _, rrow := range rctx.Results.Rows {
				matchCnt++
				ok := true
				for _, idx := range j.node.RightTmpCols {
					if !rrow[idx].IsNull() {
						ok = false
						break
					}
				}
				if ok {
					ctx.Results.Rows = append(ctx.Results.Rows, joinRows(lrow, rrow, j.node.Cols))
					ctx.Results.RowsAffected++
				}
			}
		}
		if matchCnt == 0 {
			concatLeftAndNil([][]sqltypes.Value{lrow}, j.node, ctx.Results)
		}
	}

	if wantfields {
		wantfields = false
		for k := range j.node.Vars {
			joinVars[k] = sqltypes.NullBindVariable
		}
		if err = j.right.getFields(rctx, combineVars(bindVars, joinVars)); err != nil {
			return err
		}
		ctx.Results.Fields = joinFields(lctx.Results.Fields, rctx.Results.Fields, j.node.Cols)
	}
	return nil
}

// getFields fetches the field info.
func (j *JoinEngine) getFields(ctx *xcontext.ResultContext, bindVars map[string]*querypb.BindVariable) error {
	var err error
	lctx := xcontext.NewResultContext()
	rctx := xcontext.NewResultContext()

	joinVars := make(map[string]*querypb.BindVariable)
	if err = j.left.getFields(lctx, bindVars); err != nil {
		return err
	}

	for k := range j.node.Vars {
		joinVars[k] = sqltypes.NullBindVariable
	}
	if err = j.right.getFields(rctx, bindVars); err != nil {
		return err
	}

	ctx.Results = &sqltypes.Result{}
	ctx.Results.Fields = joinFields(lctx.Results.Fields, rctx.Results.Fields, j.node.Cols)
	return nil
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

func combineVars(bv1, bv2 map[string]*querypb.BindVariable) map[string]*querypb.BindVariable {
	out := make(map[string]*querypb.BindVariable)
	for k, v := range bv1 {
		out[k] = v
	}
	for k, v := range bv2 {
		out[k] = v
	}
	return out
}

// cartesianProduct used to produce cartesian product.
func cartesianProduct(lres, rres, res *sqltypes.Result, node *planner.JoinNode) {
	for _, lrow := range lres.Rows {
		for _, rrow := range rres.Rows {
			res.Rows = append(res.Rows, joinRows(lrow, rrow, node.Cols))
			res.RowsAffected++
		}
	}
}
