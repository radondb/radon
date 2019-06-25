/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package executor

import (
	"sort"
	"sync"

	"planner"

	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// sortMergeJoin used to join `lres` and `rres` to `res`.
func sortMergeJoin(lres, rres, res *sqltypes.Result, node *planner.JoinNode) {
	var wg sync.WaitGroup
	sort := func(keys []planner.JoinKey, res *sqltypes.Result) {
		defer wg.Done()
		sort.Slice(res.Rows, func(i, j int) bool {
			for _, key := range keys {
				cmp := sqltypes.NullsafeCompare(res.Rows[i][key.Index], res.Rows[j][key.Index])
				if cmp == 0 {
					continue
				}
				return cmp < 0
			}
			return true
		})
	}
	wg.Add(1)
	go sort(node.LeftKeys, lres)
	wg.Add(1)
	go sort(node.RightKeys, rres)
	wg.Wait()

	mergeJoin(lres, rres, res, node)
}

// mergeJoin used to join the sorted results.
func mergeJoin(lres, rres, res *sqltypes.Result, node *planner.JoinNode) {
	lrows, lidx := fetchSameKeyRows(lres.Rows, node.LeftKeys, 0)
	rrows, ridx := fetchSameKeyRows(rres.Rows, node.RightKeys, 0)
	for lrows != nil {
		if rrows == nil {
			concatLeftAndNil(lres.Rows[lidx-len(lrows):], node, res)
			break
		}

		cmp := 0
		isNull := false
		for k, key := range node.LeftKeys {
			cmp = sqltypes.NullsafeCompare(lrows[0][key.Index], rrows[0][node.RightKeys[k].Index])
			if cmp != 0 {
				break
			}
			if lrows[0][key.Index].IsNull() {
				isNull = true
				break
			}
		}

		if cmp == 0 {
			if isNull {
				concatLeftAndNil(lrows, node, res)
			} else {
				concatLeftAndRight(lrows, rrows, node, res)
			}
			lrows, lidx = fetchSameKeyRows(lres.Rows, node.LeftKeys, lidx)
			rrows, ridx = fetchSameKeyRows(rres.Rows, node.RightKeys, ridx)
		} else if cmp > 0 {
			rrows, ridx = fetchSameKeyRows(rres.Rows, node.RightKeys, ridx)
		} else {
			concatLeftAndNil(lrows, node, res)
			lrows, lidx = fetchSameKeyRows(lres.Rows, node.LeftKeys, lidx)
		}
	}
}

// fetchSameKeyRows used to fetch the same joinkey values' rows.
func fetchSameKeyRows(rows [][]sqltypes.Value, joins []planner.JoinKey, index int) ([][]sqltypes.Value, int) {
	var chunk [][]sqltypes.Value
	if index >= len(rows) {
		return nil, index
	}

	if len(joins) == 0 {
		return rows, len(rows)
	}

	current := rows[index]
	chunk = append(chunk, current)
	index++
	for index < len(rows) {
		equal := keysEqual(current, rows[index], joins)
		if !equal {
			break
		}

		chunk = append(chunk, rows[index])
		index++
	}
	return chunk, index
}

func keysEqual(row1, row2 []sqltypes.Value, joins []planner.JoinKey) bool {
	for _, join := range joins {
		cmp := sqltypes.NullsafeCompare(row1[join.Index], row2[join.Index])
		if cmp != 0 {
			return false
		}
	}
	return true
}

// concatLeftAndRight used to concat thle left and right results, handle otherJoinOn|rightNull|OtherFilter.
func concatLeftAndRight(lrows, rrows [][]sqltypes.Value, node *planner.JoinNode, res *sqltypes.Result) {
	var mu sync.Mutex
	p := newCalcPool(joinWorkers)
	mathOps := func(lrow []sqltypes.Value) {
		defer p.done()
		blend := true
		matchCnt := 0
		for _, idx := range node.LeftTmpCols {
			vn := lrow[idx].ToNative()
			if vn == nil || vn.(int64) == 0 {
				blend = false
				break
			}
		}

		if blend {
			for _, rrow := range rrows {
				match := true
				for _, filter := range node.CmpFilter {
					v1, v2 := lrow[filter.Left], rrow[filter.Right]
					if filter.Exchange {
						v1, v2 = v2, v1
					}
					cmp := sqltypes.NullsafeCompare(v1, v2)
					switch filter.Operator {
					case sqlparser.EqualStr:
						if cmp != 0 {
							match = false
						}
					case sqlparser.LessThanStr:
						if cmp != -1 {
							match = false
						}
					case sqlparser.GreaterThanStr:
						if cmp != 1 {
							match = false
						}
					case sqlparser.LessEqualStr:
						if cmp == 1 {
							match = false
						}
					case sqlparser.GreaterEqualStr:
						if cmp == -1 {
							match = false
						}
					case sqlparser.NotEqualStr:
						if cmp == 0 {
							match = false
						}
					case sqlparser.NullSafeEqualStr:
						if cmp != 0 {
							match = false
						}
					}
					if !match {
						break
					}
					// null value cannot match.
					if filter.Operator != sqlparser.NullSafeEqualStr && (lrow[filter.Left].IsNull() || rrow[filter.Right].IsNull()) {
						match = false
						break
					}
				}
				if match {
					matchCnt++
					ok := true
					for _, idx := range node.RightTmpCols {
						if !rrow[idx].IsNull() {
							ok = false
							break
						}
					}
					if ok {
						mu.Lock()
						res.Rows = append(res.Rows, joinRows(lrow, rrow, node.Cols))
						res.RowsAffected++
						mu.Unlock()
					}
				}
			}
		}
		if matchCnt == 0 && node.IsLeftJoin && !node.HasRightFilter {
			mu.Lock()
			res.Rows = append(res.Rows, joinRows(lrow, nil, node.Cols))
			res.RowsAffected++
			mu.Unlock()
		}
	}

	for _, lrow := range lrows {
		p.add(1)
		go mathOps(lrow)
	}
	p.wait()
}

func concatLeftAndNil(lrows [][]sqltypes.Value, node *planner.JoinNode, res *sqltypes.Result) {
	if node.IsLeftJoin && !node.HasRightFilter {
		for _, row := range lrows {
			res.Rows = append(res.Rows, joinRows(row, nil, node.Cols))
			res.RowsAffected++
		}
	}
}

// calcPool used to the merge join calc.
type calcPool struct {
	queue chan int
	wg    *sync.WaitGroup
}

func newCalcPool(size int) *calcPool {
	if size <= 0 {
		size = 1
	}
	return &calcPool{
		queue: make(chan int, size),
		wg:    &sync.WaitGroup{},
	}
}

func (p *calcPool) add(delta int) {
	for i := 0; i < delta; i++ {
		p.queue <- 1
	}
	for i := 0; i > delta; i-- {
		<-p.queue
	}
	p.wg.Add(delta)
}

func (p *calcPool) done() {
	<-p.queue
	p.wg.Done()
}

func (p *calcPool) wait() {
	p.wg.Wait()
}
