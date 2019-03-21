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

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/hack"
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
			if node.IsLeftJoin && !node.HasRightFilter {
				for _, row := range lres.Rows[lidx-len(lrows):] {
					res.Rows = append(res.Rows, joinRows(row, nil, node.Cols))
					res.RowsAffected++
				}
			}
			break
		}

		cmp := 0
		for k, key := range node.LeftKeys {
			cmp = sqltypes.Compare(lrows[0][key.Index], rrows[0][node.RightKeys[k].Index])
			if cmp != 0 {
				break
			}
		}
		if cmp == 0 {
			if len(node.LeftTmpCols) > 0 {
				for _, lrow := range lrows {
					match := true
					for _, idx := range node.LeftTmpCols {
						vn := lrow[idx].ToNative()
						if vn.(int64) == 0 {
							match = false
							break
						}
					}
					if match {
						for _, rrow := range rrows {
							res.Rows = append(res.Rows, joinRows(lrow, rrow, node.Cols))
							res.RowsAffected++
						}
					} else {
						if !node.HasRightFilter {
							res.Rows = append(res.Rows, joinRows(lrow, nil, node.Cols))
							res.RowsAffected++
						}
					}
				}
			} else {
				for _, lrow := range lrows {
					for _, rrow := range rrows {
						res.Rows = append(res.Rows, joinRows(lrow, rrow, node.Cols))
						res.RowsAffected++
					}
				}
			}
			lrows, lidx, lstr = fetchSameKeyRows(lres.Rows, node.LeftKeys, lidx, lstr, node.LeftUnique)
			rrows, ridx, rstr = fetchSameKeyRows(rres.Rows, node.RightKeys, ridx, rstr, node.RightUnique)
		} else if cmp > 0 {
			rrows, ridx, rstr = fetchSameKeyRows(rres.Rows, node.RightKeys, ridx, rstr, node.RightUnique)
		} else {
			if node.IsLeftJoin && !node.HasRightFilter {
				for _, row := range lrows {
					res.Rows = append(res.Rows, joinRows(row, nil, node.Cols))
					res.RowsAffected++
				}
			}
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

	if isUnique {
		return rows[index : index+1], index + 1, ""
	}

	if str == "" {
		keySlice := []byte{0x01}
		for _, join := range joins {
			keySlice = append(keySlice, rows[index][join.Index].Raw()...)
			keySlice = append(keySlice, 0x02)
		}
		str = hack.String(keySlice)
	}
	chunk = append(chunk, rows[index])
	index++

	for index < len(rows) {
		keySlice := []byte{0x01}
		for _, join := range joins {
			keySlice = append(keySlice, rows[index][join.Index].Raw()...)
			keySlice = append(keySlice, 0x02)
		}
		key = hack.String(keySlice)

		if key != str {
			break
		}
		chunk = append(chunk, rows[index])
		index++
	}
	return chunk, index, key
}
