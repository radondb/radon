/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package engine

import (
	"sort"
	"sync"

	"planner/builder"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

const (
	// joinWorkers used for merge join.
	joinWorkers = 4
)

// sortMergeJoin used to join `lres` and `rres` to `res`.
func sortMergeJoin(lres, rres, res *sqltypes.Result, node *builder.JoinNode, maxrow int) error {
	var wg sync.WaitGroup
	sort := func(keys []builder.JoinKey, res *sqltypes.Result) {
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

	return mergeJoin(lres, rres, res, node, maxrow)
}

// mergeJoin used to join the sorted results.
func mergeJoin(lres, rres, res *sqltypes.Result, node *builder.JoinNode, maxrow int) error {
	var err error
	lrows, lidx := fetchSameKeyRows(lres.Rows, node.LeftKeys, 0)
	rrows, ridx := fetchSameKeyRows(rres.Rows, node.RightKeys, 0)
	for lrows != nil {
		if rrows == nil {
			err = concatLeftAndNil(lres.Rows[lidx-len(lrows):], node, res, maxrow)
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
				err = concatLeftAndNil(lrows, node, res, maxrow)
			} else {
				err = concatLeftAndRight(lrows, rrows, node, res, maxrow)
			}

			lrows, lidx = fetchSameKeyRows(lres.Rows, node.LeftKeys, lidx)
			rrows, ridx = fetchSameKeyRows(rres.Rows, node.RightKeys, ridx)
		} else if cmp > 0 {
			rrows, ridx = fetchSameKeyRows(rres.Rows, node.RightKeys, ridx)
		} else {
			err = concatLeftAndNil(lrows, node, res, maxrow)
			lrows, lidx = fetchSameKeyRows(lres.Rows, node.LeftKeys, lidx)
		}

		if err != nil {
			return err
		}
	}
	return err
}

// fetchSameKeyRows used to fetch the same joinkey values' rows.
func fetchSameKeyRows(rows [][]sqltypes.Value, joins []builder.JoinKey, index int) ([][]sqltypes.Value, int) {
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

func keysEqual(row1, row2 []sqltypes.Value, joins []builder.JoinKey) bool {
	for _, join := range joins {
		cmp := sqltypes.NullsafeCompare(row1[join.Index], row2[join.Index])
		if cmp != 0 {
			return false
		}
	}
	return true
}

// concatLeftAndRight used to concat the left and right results, handle otherLeftJoin|rightNull|OtherFilter.
func concatLeftAndRight(lrows, rrows [][]sqltypes.Value, node *builder.JoinNode, res *sqltypes.Result, maxrow int) error {
	var err error
	var mu sync.Mutex
	p := newCalcPool(joinWorkers)
	mathOps := func(lrow []sqltypes.Value) {
		defer p.done()
		if err != nil {
			return
		}

		leftMatch := true
		matchCnt := 0
		for _, idx := range node.LeftTmpCols {
			if !sqltypes.CastToBool(lrow[idx]) {
				leftMatch = false
				break
			}
		}

		if leftMatch {
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
						if err == nil {
							res.Rows = append(res.Rows, joinRows(lrow, rrow, node.Cols))
							res.RowsAffected++
							if len(res.Rows) > maxrow {
								err = errors.Errorf("unsupported: join.row.count.exceeded.allowed.limit.of.'%d'", maxrow)
								mu.Unlock()
								break
							}
						}
						mu.Unlock()
					}
				}
			}
		}
		if matchCnt == 0 && node.IsLeftJoin && !node.HasRightFilter {
			mu.Lock()
			if err == nil {
				res.Rows = append(res.Rows, joinRows(lrow, nil, node.Cols))
				res.RowsAffected++
				if len(res.Rows) > maxrow {
					err = errors.Errorf("unsupported: join.row.count.exceeded.allowed.limit.of.'%d'", maxrow)
				}
			}
			mu.Unlock()
		}
	}

	for _, lrow := range lrows {
		p.add(1)
		go mathOps(lrow)
	}
	p.wait()

	return err
}

func concatLeftAndNil(lrows [][]sqltypes.Value, node *builder.JoinNode, res *sqltypes.Result, maxrow int) error {
	if node.IsLeftJoin && !node.HasRightFilter {
		for _, row := range lrows {
			res.Rows = append(res.Rows, joinRows(row, nil, node.Cols))
			res.RowsAffected++
			if len(res.Rows) > maxrow {
				return errors.Errorf("unsupported: join.row.count.exceeded.allowed.limit.of.'%d'", maxrow)
			}
		}
	}
	return nil
}
