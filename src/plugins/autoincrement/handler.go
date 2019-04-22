/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package autoincrement

import (
	"strconv"

	"config"

	"github.com/xelabs/go-mysqlstack/sqlparser"
)

type AutoIncrementHandler interface {
	Init() error
	Process(database string, ins *sqlparser.Insert) error
	Close() error
}

// GetAutoIncrement -- used to get config AutoIncrement from 'create table' DDL sqlnode.
func GetAutoIncrement(node *sqlparser.DDL) *config.AutoIncrement {
	switch node.Action {
	case sqlparser.CreateTableStr:
		for _, col := range node.TableSpec.Columns {
			if col.Type.Autoincrement {
				return &config.AutoIncrement{
					Column: col.Name.String(),
				}
			}
		}
	}
	return nil
}

func modifyForAutoinc(ins *sqlparser.Insert, autoinc *config.AutoIncrement, seq uint64) {
	col := sqlparser.NewColIdent(autoinc.Column)

	// Insert has autoinc column.
	for _, column := range ins.Columns {
		if col.Equal(column) {
			return
		}
	}

	// Insert does not has autoinc column
	// 1. append column info to the end.
	ins.Columns = append(ins.Columns, col)

	// 2. append vals to each row's end.
	rows := ins.Rows.(sqlparser.Values)
	for i := range rows {
		seq++
		rows[i] = append(rows[i], sqlparser.NewIntVal([]byte(strconv.FormatUint(seq, 10))))
	}
}
