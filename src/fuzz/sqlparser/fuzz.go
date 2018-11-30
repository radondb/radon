// Copyright 2015 Dmitry Vyukov. All rights reserved.
// Use of this source code is governed by Apache 2 LICENSE that can be found in the LICENSE file.
// Copyright 2018 The Radon Authors.

package sqlparser

import (
	"fmt"

	"github.com/xelabs/go-mysqlstack/sqlparser"

	"github.com/dvyukov/go-fuzz-corpus/fuzz"
)

func parseAll(data []byte) ([]sqlparser.Statement, error) {
	stmt, err := sqlparser.Parse(string(data))
	return []sqlparser.Statement{stmt}, err
}

// stringAndParse turns the Statement into a SQL string, re-parses
// that string, and checks the result matches the original.
func stringAndParse(data []byte, stmt sqlparser.Statement) {
	data1 := sqlparser.String(stmt)
	stmt1, err := sqlparser.Parse(data1)
	if err != nil {
		fmt.Printf("data0: %q\n", data)
		fmt.Printf("data1: %q\n", data1)
		panic(err)
	}
	if !fuzz.DeepEqual(stmt, stmt1) {
		fmt.Printf("data0: %q\n", data)
		fmt.Printf("data1: %q\n", data1)
		panic("not equal")
	}
}

func Fuzz(data []byte) int {
	stmts, err := parseAll(data)
	if err != nil {
		return 0
	}
	for _, stmt := range stmts {
		stringAndParse(data, stmt)

		if sel, ok := stmt.(*sqlparser.Select); ok {
			var nodes []sqlparser.SQLNode
			for _, x := range sel.From {
				nodes = append(nodes, x)
			}
			for _, x := range sel.SelectExprs {
				nodes = append(nodes, x)
			}
			for _, x := range sel.GroupBy {
				nodes = append(nodes, x)
			}
			for _, x := range sel.OrderBy {
				nodes = append(nodes, x)
			}
			nodes = append(nodes, sel.Where)
			nodes = append(nodes, sel.Having)
			nodes = append(nodes, sel.Limit)
			for _, n := range nodes {
				if n == nil {
					continue
				}
				if x, ok := n.(sqlparser.SimpleTableExpr); ok {
					sqlparser.GetTableName(x)
				}
				if x, ok := n.(sqlparser.Expr); ok {
					sqlparser.IsColName(x)
					sqlparser.IsValue(x)
					sqlparser.IsNull(x)
					sqlparser.IsSimpleTuple(x)
				}
			}
		}
	}
	return 1
}
