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

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/hack"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// simpleBNJoin is the simple block nested loop join.
func simpleBNJoin(lrows, rrows [][]sqltypes.Value, res *sqltypes.Result, node *planner.JoinNode) {
	exchange := false
	irows, orows := lrows, rrows
	ikeys, okeys := node.LeftKeys, node.RightKeys
	if len(lrows) > len(rrows) {
		irows, orows = orows, irows
		ikeys, okeys = okeys, ikeys
		exchange = true
	}
	inner := make(map[string][][]sqltypes.Value)
	for _, row := range irows {
		keySlice := []byte{0x01}
		isNull := false
		for _, key := range ikeys {
			if row[key.Index].IsNull() {
				isNull = true
				break
			}
			keySlice = append(keySlice, row[key.Index].Raw()...)
			keySlice = append(keySlice, 0x02)
		}
		if isNull {
			continue
		}
		key := hack.String(keySlice)
		inner[key] = append(inner[key], row)
	}

	if len(inner) == 0 {
		return
	}
	for _, row2 := range orows {
		keySlice := []byte{0x01}
		isNull := false
		for _, key := range okeys {
			if row2[key.Index].IsNull() {
				isNull = true
				break
			}
			keySlice = append(keySlice, row2[key.Index].Raw()...)
			keySlice = append(keySlice, 0x02)
		}
		if isNull {
			continue
		}
		key := hack.String(keySlice)
		if rows, ok := inner[key]; ok {
			for _, row1 := range rows {
				if exchange {
					res.Rows = append(res.Rows, joinRows(row2, row1, node.Cols))
				} else {
					res.Rows = append(res.Rows, joinRows(row1, row2, node.Cols))
				}
				res.RowsAffected++
			}
		}
	}
}
