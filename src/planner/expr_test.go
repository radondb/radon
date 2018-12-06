/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"router"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestGetDMLRouting(t *testing.T) {
	querys := []string{
		"select * from B where B.b between 10 and 20 and B.id = 10",
		"select * from B where id = 10",
		"select * from A join B on A.id = B.id where A.id = 10",
	}

	want := []int{
		1,
		1,
		2,
	}
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableBConfig(), router.MockTableMConfig())
	assert.Nil(t, err)

	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		n := node.(*sqlparser.Select)
		assert.Nil(t, err)
		got, err := getDMLRouting(database, "B", "id", n.Where, route)
		assert.Nil(t, err)
		assert.Equal(t, want[i], len(got))
	}
}

func TestParserSelectExprsSubquery(t *testing.T) {
	query := "select A.*,(select b.str from b where A.id=B.id) str from A"
	want := "unsupported: subqueries.in.select.exprs"

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"
	route, cleanup := router.MockNewRouter(log)
	defer cleanup()
	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig())
	assert.Nil(t, err)

	node, err := sqlparser.Parse(query)
	assert.Nil(t, err)

	sel := node.(*sqlparser.Select)
	_, err = parserSelectExprs(sel.SelectExprs)
	got := err.Error()
	assert.Equal(t, want, got)
}
