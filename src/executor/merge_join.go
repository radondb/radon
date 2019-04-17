/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package executor

import (
	"planner"
	"sync"

	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// sortMergeJoin used to join `lres` and `rres` to `res`.
func sortMergeJoin(lres, rres, res *sqltypes.Result, node *planner.JoinNode) {
	var wg sync.WaitGroup
	sort := func(keys []planner.JoinKey, res *sqltypes.Result) {
		defer wg.Done()
		for _, key := range keys {
			res.OrderedByAsc(key.Table, key.Field)
		}
		res.Sort()
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
	lrows, lidx, lstr := fetchSameKeyRows(lres.Rows, node.LeftKeys, 0, "", node.LeftUnique)
	rrows, ridx, rstr := fetchSameKeyRows(rres.Rows, node.RightKeys, 0, "", node.RightUnique)
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
			lrows, lidx, lstr = fetchSameKeyRows(lres.Rows, node.LeftKeys, lidx, lstr, node.LeftUnique)
			rrows, ridx, rstr = fetchSameKeyRows(rres.Rows, node.RightKeys, ridx, rstr, node.RightUnique)
		} else if cmp > 0 {
			rrows, ridx, rstr = fetchSameKeyRows(rres.Rows, node.RightKeys, ridx, rstr, node.RightUnique)
		} else {
			concatLeftAndNil(lrows, node, res)
			lrows, lidx, lstr = fetchSameKeyRows(lres.Rows, node.LeftKeys, lidx, lstr, node.LeftUnique)
		}
	}
}

// fetchSameKeyRows used to fetch the same joinkey values' rows.
func fetchSameKeyRows(rows [][]sqltypes.Value, joins []planner.JoinKey, index int, str string, isUnique bool) ([][]sqltypes.Value, int, string) {
	var chunk [][]sqltypes.Value
	var key string
	if index >= len(rows) {
		return nil, index, ""
	}

	if len(joins) == 0 {
		return rows, len(rows), ""
	}

	if isUnique {
		return rows[index : index+1], index + 1, ""
	}

	if str == "" {
		keySlice := []byte{0x01}
		for _, join := range joins {
			keySlice = append(keySlice, rows[index][join.Index].Raw()...)
			keySlice = append(keySlice, 0x02)
		}
		str = common.BytesToString(keySlice)
	}
	chunk = append(chunk, rows[index])
	index++

	for index < len(rows) {
		keySlice := []byte{0x01}
		for _, join := range joins {
			keySlice = append(keySlice, rows[index][join.Index].Raw()...)
			keySlice = append(keySlice, 0x02)
		}
		key = common.BytesToString(keySlice)

		if key != str {
			break
		}
		chunk = append(chunk, rows[index])
		index++
	}
	return chunk, index, key
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
			if vn.(int64) == 0 {
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
