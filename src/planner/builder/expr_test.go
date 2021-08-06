/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

import (
	"backend"
	"testing"

	"router"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestLookupFromWhere(t *testing.T) {
	querys := []string{
		"select * from B where B.b between 10 and 20 and B.id = 10",
		"select * from B where id = 10",
		"select * from A join B on A.id = B.id where A.id = 10",
		"select * from B where B.id in (1,2,3)",
		"select * from B where id = 1 or id =2 or id =3",
		"select * from B where B.id in (1,2,c)",
		"select * from B where ID = 10",
	}

	want := []int{
		1,
		1,
		2,
		1,
		1,
		2,
		1,
	}
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableBConfig(), router.MockTableMConfig())
	assert.Nil(t, err)

	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		n := node.(*sqlparser.Select)
		assert.Nil(t, err)
		got, err := LookupFromWhere(database, "B", "id", n.Where, route)
		assert.Nil(t, err)
		assert.Equal(t, want[i], len(got))
	}
}

func TestLookupFromWhereErr(t *testing.T) {
	testcases := []struct {
		query string
		out   string
	}{
		{
			query: "select * from B where B.id in (1,2,0x12)",
			out:   "hash.unsupported.key.type:[3]",
		},
	}
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableBConfig())
	assert.Nil(t, err)

	for _, testcase := range testcases {
		node, err := sqlparser.Parse(testcase.query)
		n := node.(*sqlparser.Select)
		assert.Nil(t, err)
		_, err = LookupFromWhere(database, "B", "id", n.Where, route)
		assert.NotNil(t, err)
		assert.Equal(t, testcase.out, err.Error())
	}
}

func TestParserWhereOrJoinExprs(t *testing.T) {
	querys := []string{
		"select * from A where id=1",
		"select * from A where concat(A.str1,A.str2)='sansi'",
		"select * from A where 1=A.id",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig())
	assert.Nil(t, err)

	for _, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)
		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(sel.From)
		assert.Nil(t, err)

		_, _, err = parseWhereOrJoinExprs(sel.Where.Expr, b.root.getReferTables())
		assert.Nil(t, err)
	}
}

func TestWhereFilters(t *testing.T) {
	querys := []string{
		"select * from G, A where G.id=A.id and A.id=1",
		"select * from G, A, A as B where A.a=B.a and A.id=B.id and A.b=B.b",
		"select * from A, A as B where A.a>B.a and A.a=B.a and A.id=1 and B.id=1 and 1=1",
		"select * from G, A join A as B on A.a=B.a where A.b=B.b and A.id=1 and B.id=1",
		"select * from (A join A as B on A.a>B.a and 1=1),G where A.id=B.id",
		"select * from G,A,B where 1=1 and A.id=1",
		"select * from A left join A as B on A.a = B.a where A.b = B.b and A.id=B.id",
		"select * from A join B on A.id=B.id where concat(A.str1,A.str2)='sansi'",
	}
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)

	for _, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)
		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(sel.From)
		assert.Nil(t, err)

		err = b.pushFilters(sel.Where.Expr)
		assert.Nil(t, err)

		_, err = b.root.calcRoute()
		assert.Nil(t, err)

		assert.Nil(t, err)
	}
}

func TestWhereFiltersError(t *testing.T) {
	query := "select * from A where id=0x12"
	want := "hash.unsupported.key.type:[3]"
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)
	b := NewPlanBuilder(log, route, nil, "sbtest")

	node, err := sqlparser.Parse(query)
	assert.Nil(t, err)
	sel := node.(*sqlparser.Select)

	b.root, err = b.scanTableExprs(sel.From)
	assert.Nil(t, err)

	// where filter error.
	{
		err = b.pushFilters(sel.Where.Expr)
		got := err.Error()
		assert.Equal(t, want, got)
	}
	// check shard error.
	{
		_, err = b.checkShard("B", "id")
		assert.Equal(t, "unsupported: unknown.column.'B.id'.in.field.list", err.Error())
	}
	// get on tableinfo.
	{
		getOneTableInfo(nil)
	}
	// splitAndExpression.
	{
		splitAndExpression(nil, nil)
	}
}

func TestParserHaving(t *testing.T) {
	querys := []string{
		"select * from A where A.id=1 having concat(id,name) = 'sansi'",
		"select A.id from G, A where G.id=A.id having A.id=1",
		"select A.a from A, B where A.id=B.id having A.a=1 and 1=1",
		"select G.id, B.id, B.a from A,G,B where A.id=B.id having G.id=B.id and B.a=1 and 1=1",
		"select A.a from A,B where A.id=1 having a>1",
	}
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	scatter, fakedbs, cleanup := backend.MockScatter(log, 10)
	defer cleanup()
	fakedbs.AddQueryPattern("desc .*", descResult)

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)

	for _, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)
		b := NewPlanBuilder(log, route, scatter, "sbtest")
		b.root, err = b.scanTableExprs(sel.From)
		assert.Nil(t, err)

		fields, aggTyp, err := parseSelectExprs(scatter, b.root, b.tables, &sel.SelectExprs)
		assert.Nil(t, err)

		err = b.root.pushSelectExprs(fields, nil, sel, aggTyp)
		assert.Nil(t, err)

		err = b.pushHavings(sel.Having.Expr)
		assert.Nil(t, err)
	}
}

func TestParserHavingError(t *testing.T) {
	querys := []string{
		"select G.id, B.id, B.a from G,A,B where A.id=B.id having G.id=B.id and B.a=1 and 1=1",
		"select B.id from A,B where A.id=1 having sum(B.id)>10",
		"select A.a from A,B where A.id=1 having C.a>1",
	}
	wants := []string{
		"unsupported: havings.'G.id = B.id'.in.cross-shard.join",
		"unsupported: expr[sum(B.id)].in.having.clause",
		"unsupported: unknown.column.'C.a'.in.having.clause",
	}
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)

	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)
		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(sel.From)
		assert.Nil(t, err)

		fields, aggTyp, err := parseSelectExprs(nil, b.root, b.tables, &sel.SelectExprs)
		assert.Nil(t, err)

		err = b.root.pushSelectExprs(fields, nil, sel, aggTyp)
		assert.Nil(t, err)

		err = b.pushHavings(sel.Having.Expr)
		got := err.Error()
		assert.Equal(t, wants[i], got)
	}
}
