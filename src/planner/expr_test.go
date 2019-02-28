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

	err := route.AddForTest(database, router.MockTableMConfig())
	assert.Nil(t, err)

	for _, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)

		p, err := scanTableExprs(log, route, database, sel.From)
		assert.Nil(t, err)

		_, _, err = parserWhereOrJoinExprs(sel.Where.Expr, p.getReferredTables())
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
	}
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)

	for _, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)

		p, err := scanTableExprs(log, route, database, sel.From)
		assert.Nil(t, err)

		joins, filters, err := parserWhereOrJoinExprs(sel.Where.Expr, p.getReferredTables())
		assert.Nil(t, err)

		err = p.pushFilter(filters)
		assert.Nil(t, err)

		p, err = p.pushJoinInWhere(joins)
		assert.Nil(t, err)

		p, err = p.calcRoute()
		assert.Nil(t, err)

		err = p.spliceWhere()
		assert.Nil(t, err)
	}
}

func TestWhereFiltersError(t *testing.T) {
	querys := []string{
		"select * from G,A,B where A.id=B.id and A.a > B.a",
		"select * from A join B on A.id=B.id join G on G.id=A.id where A.a>B.a",
	}
	wants := []string{
		"unsupported: where.clause.in.cross-shard.join",
		"unsupported: where.clause.in.cross-shard.join",
	}
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)

	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)

		p, err := scanTableExprs(log, route, database, sel.From)
		assert.Nil(t, err)

		joins, filters, err := parserWhereOrJoinExprs(sel.Where.Expr, p.getReferredTables())
		assert.Nil(t, err)

		err = p.pushFilter(filters)
		assert.Nil(t, err)

		p, err = p.pushJoinInWhere(joins)
		assert.Nil(t, err)

		p, err = p.calcRoute()
		assert.Nil(t, err)

		err = p.spliceWhere()
		got := err.Error()
		assert.Equal(t, wants[i], got)
	}
}

func TestCheckGroupBy(t *testing.T) {
	querys := []string{
		"select a,b from A group by a",
		"select a,b from A group by a,b",
		"select a,b,A.id from A group by id,a",
		"select A.id as a from A group by a",
	}
	wants := []int{
		1,
		2,
		0,
		1,
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)

	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)

		p, err := scanTableExprs(log, route, database, sel.From)
		assert.Nil(t, err)

		fields, err := parserSelectExprs(sel.SelectExprs)
		assert.Nil(t, err)

		groups, err := checkGroupBy(sel.GroupBy, fields, route, database, p.getReferredTables())
		assert.Nil(t, err)
		assert.Equal(t, wants[i], len(groups))
	}
}

func TestCheckGroupByError(t *testing.T) {
	querys := []string{
		"select a,b from A group by B.a",
		"select a,b from A group by 1",
		"select a,b from A group by a,id",
	}
	wants := []string{
		"unsupported: unknow.table.in.group.by.field[B.a]",
		"unsupported: group.by.field.have.expression",
		"unsupported: group.by.field[id].should.be.in.select.list",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)

	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)

		p, err := scanTableExprs(log, route, database, sel.From)
		assert.Nil(t, err)

		fields, err := parserSelectExprs(sel.SelectExprs)
		assert.Nil(t, err)

		_, err = checkGroupBy(sel.GroupBy, fields, route, database, p.getReferredTables())
		got := err.Error()
		assert.Equal(t, wants[i], got)
	}
}

func TestCheckDistinct(t *testing.T) {
	querys := []string{
		"select distinct A.a,A.b as c from A",
		"select distinct A.id from A",
		"select distinct A.a,A.b,A.c from A group by a",
	}
	wants := []int{
		2,
		0,
		1,
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)

	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)

		p, err := scanTableExprs(log, route, database, sel.From)
		assert.Nil(t, err)

		fields, err := parserSelectExprs(sel.SelectExprs)
		assert.Nil(t, err)

		_, err = checkDistinct(sel, nil, fields, route, database, p.getReferredTables())
		assert.Nil(t, err)
		assert.Equal(t, wants[i], len(sel.GroupBy))
	}
}

func TestCheckDistinctError(t *testing.T) {
	querys := []string{
		"select distinct * from A",
		"select distinct A.a+1 as a, A.b*10 from A",
	}
	wants := []string{
		"unsupported: distinct",
		"unsupported: distinct",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)

	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)

		p, err := scanTableExprs(log, route, database, sel.From)
		assert.Nil(t, err)

		fields, err := parserSelectExprs(sel.SelectExprs)
		assert.Nil(t, err)

		_, err = checkDistinct(sel, nil, fields, route, database, p.getReferredTables())
		got := err.Error()
		assert.Equal(t, wants[i], got)
	}
}

func TestSelectExprs(t *testing.T) {
	querys := []string{
		"select A.id,G.a as a, concat(B.str,G.str), 1 from A,B,G group by a",
		"select A.id, G.a as a from A,G group by a",
	}
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)

	for _, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)

		p, err := scanTableExprs(log, route, database, sel.From)
		assert.Nil(t, err)

		fields, err := parserSelectExprs(sel.SelectExprs)
		assert.Nil(t, err)

		groups, err := checkGroupBy(sel.GroupBy, fields, route, database, p.getReferredTables())
		assert.Nil(t, err)

		err = p.pushSelectExprs(fields, groups, sel, false)
		assert.Nil(t, err)
	}
}

func TestSelectExprsError(t *testing.T) {
	querys := []string{
		"select sum(A.id) as s, G.a as a from A,G group by s",
		"select A.id,G.a as a, concat(B.str,G.str), 1 from A,B, A as G group by a",
	}
	wants := []string{
		"unsupported: group.by.field[s].should.be.in.select.list",
		"unsupported: select.expr.in.cross-shard.join",
	}
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)

	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)

		p, err := scanTableExprs(log, route, database, sel.From)
		assert.Nil(t, err)

		fields, err := parserSelectExprs(sel.SelectExprs)
		assert.Nil(t, err)

		groups, err := checkGroupBy(sel.GroupBy, fields, route, database, p.getReferredTables())
		assert.Nil(t, err)

		err = p.pushSelectExprs(fields, groups, sel, false)
		got := err.Error()
		assert.Equal(t, wants[i], got)
	}
	{
		query := "select sum(A.id) from A join B on A.id=B.id"
		want := "unsupported: cross-shard.query.with.aggregates"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)

		p, err := scanTableExprs(log, route, database, sel.From)
		assert.Nil(t, err)

		fields, err := parserSelectExprs(sel.SelectExprs)
		assert.Nil(t, err)

		groups, err := checkGroupBy(sel.GroupBy, fields, route, database, p.getReferredTables())
		assert.Nil(t, err)

		err = p.pushSelectExprs(fields, groups, sel, true)
		got := err.Error()
		assert.Equal(t, want, got)
	}
}
