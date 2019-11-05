/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

import (
	"testing"

	"router"

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
		got, err := GetDMLRouting(database, "B", "id", n.Where, route)
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
	p, err := scanTableExprs(log, route, database, sel.From)
	assert.Nil(t, err)
	_, _, err = parserSelectExprs(sel.SelectExprs, p)
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

		_, _, err = parserWhereOrJoinExprs(sel.Where.Expr, p.getReferTables())
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

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)

	for _, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)

		p, err := scanTableExprs(log, route, database, sel.From)
		assert.Nil(t, err)

		joins, filters, err := parserWhereOrJoinExprs(sel.Where.Expr, p.getReferTables())
		assert.Nil(t, err)

		err = p.pushFilter(filters)
		assert.Nil(t, err)

		p = p.pushEqualCmpr(joins)

		_, err = p.calcRoute()
		assert.Nil(t, err)

		assert.Nil(t, err)
	}
}

func TestCheckGroupBy(t *testing.T) {
	querys := []string{
		"select a,b from A group by a",
		"select a,b from A group by a,b",
		"select a,b,A.id from A group by id,a",
		"select A.id as a from A group by a",
		"select A.id+G.id as id from A,G group by id",
		"select A.id from A group by id",
		"select id as a from A group by id",
		"select id as a from A group by A.id",
	}
	wants := []int{
		1,
		2,
		0,
		0,
		1,
		0,
		0,
		0,
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

		fields, _, err := parserSelectExprs(sel.SelectExprs, p)
		assert.Nil(t, err)

		_, ok := p.(*MergeNode)
		groups, err := checkGroupBy(sel.GroupBy, fields, route, p.getReferTables(), ok)
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
		"unsupported: group.by.[1].type.should.be.colname",
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

		fields, _, err := parserSelectExprs(sel.SelectExprs, p)
		assert.Nil(t, err)

		_, ok := p.(*MergeNode)
		_, err = checkGroupBy(sel.GroupBy, fields, route, p.getReferTables(), ok)
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
		1,
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

		fields, _, err := parserSelectExprs(sel.SelectExprs, p)
		assert.Nil(t, err)

		_, ok := p.(*MergeNode)
		_, err = checkDistinct(sel, nil, fields, route, p.getReferTables(), ok)
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

		fields, _, err := parserSelectExprs(sel.SelectExprs, p)
		assert.Nil(t, err)

		_, ok := p.(*MergeNode)
		_, err = checkDistinct(sel, nil, fields, route, p.getReferTables(), ok)
		got := err.Error()
		assert.Equal(t, wants[i], got)
	}
}

func TestSelectExprs(t *testing.T) {
	querys := []string{
		"select A.id,G.a as a, concat(B.str,G.str), 1 from A,B,G group by a",
		"select A.id, G.a as a from A,G group by a",
		"select A.id, B.name from A join B on A.id=B.id",
		"select A.id, B.name from A join B on A.id=B.id join G on G.a=A.a",
		"select sum(A.id) from A join B on A.id=B.id",
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

		fields, aggTyp, err := parserSelectExprs(sel.SelectExprs, p)
		assert.Nil(t, err)

		_, ok := p.(*MergeNode)
		groups, err := checkGroupBy(sel.GroupBy, fields, route, p.getReferTables(), ok)
		assert.Nil(t, err)

		err = p.pushSelectExprs(fields, groups, sel, aggTyp)
		assert.Nil(t, err)

		err = p.pushOrderBy(sel)
		assert.Nil(t, err)
	}
}

func TestSelectExprsError(t *testing.T) {
	querys := []string{
		"select sum(A.id) as s, G.a as a from A,G group by s",
	}
	wants := []string{
		"unsupported: group.by.field[sum(A.id)].should.be.in.noaggregate.select.list",
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

		fields, aggTyp, err := parserSelectExprs(sel.SelectExprs, p)
		assert.Nil(t, err)

		_, ok := p.(*MergeNode)
		groups, err := checkGroupBy(sel.GroupBy, fields, route, p.getReferTables(), ok)
		assert.Nil(t, err)

		err = p.pushSelectExprs(fields, groups, sel, aggTyp)
		got := err.Error()
		assert.Equal(t, wants[i], got)
	}
}

func TestParserHaving(t *testing.T) {
	querys := []string{
		"select * from A where A.id=1 having concat(str1,str2) = 'sansi'",
		"select A.id from G, A where G.id=A.id having A.id=1",
		"select A.a from A, B where A.id=B.id having A.a=1 and 1=1",
		"select G.id, B.id, B.a from A,G,B where A.id=B.id having G.id=B.id and B.a=1 and 1=1",
		"select A.a from A,B where A.id=1 having a>1",
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

		fields, aggTyp, err := parserSelectExprs(sel.SelectExprs, p)
		assert.Nil(t, err)

		err = p.pushSelectExprs(fields, nil, sel, aggTyp)
		assert.Nil(t, err)

		havings, err := parserHaving(sel.Having.Expr, p.getReferTables(), p.getFields())
		assert.Nil(t, err)

		err = p.pushHaving(havings)
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

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)

	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)

		p, err := scanTableExprs(log, route, database, sel.From)
		assert.Nil(t, err)

		fields, aggTyp, err := parserSelectExprs(sel.SelectExprs, p)
		assert.Nil(t, err)

		err = p.pushSelectExprs(fields, nil, sel, aggTyp)
		assert.Nil(t, err)

		havings, err := parserHaving(sel.Having.Expr, p.getReferTables(), p.getFields())
		if err != nil {
			got := err.Error()
			assert.Equal(t, wants[i], got)
		} else {
			err = p.pushHaving(havings)
			got := err.Error()
			assert.Equal(t, wants[i], got)
		}
	}
}

func TestPushOrderBy(t *testing.T) {
	querys := []string{
		"select * from A order by a",
		"select A.a,B.b from A join B on A.id=B.id order by A.a",
		"select A.a,B.b from A join B on A.id=B.id group by A.a",
		"select A.a from A order by a",
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

		fields, _, err := parserSelectExprs(sel.SelectExprs, p)
		assert.Nil(t, err)
		switch p := p.(type) {
		case *MergeNode:
			p.fields = fields
		case *JoinNode:
			p.fields = fields
		}

		err = p.pushOrderBy(sel)
		assert.Nil(t, err)
	}
}

func TestPushOrderByError(t *testing.T) {
	querys := []string{
		"select a from A order by b",
		"select A.a from A join B on A.id=B.id order by B.b",
		"select A.a from A join B on A.id=B.id order by C.a",
	}
	wants := []string{
		"unsupported: orderby[b].should.in.select.list",
		"unsupported: orderby[B.b].should.in.select.list",
		"unsupported: unknow.table.in.order.by.field[C.a]",
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

		fields, _, err := parserSelectExprs(sel.SelectExprs, p)
		assert.Nil(t, err)
		switch p := p.(type) {
		case *MergeNode:
			p.fields = fields
		case *JoinNode:
			p.fields = fields
		}

		err = p.pushOrderBy(sel)
		got := err.Error()
		assert.Equal(t, wants[i], got)
	}
}

func TestPushLimit(t *testing.T) {
	querys := []string{
		"select * from A limit 2",
		"select * from A limit 2,2",
		"select A.a,B.b from A join B on A.id=B.id order by A.a limit 2",
		"select A.a,B.b from A join B on A.id=B.id group by A.a limit 2,2",
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

		err = p.pushLimit(sel)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(p.Children()))
	}
}

func TestPushLimitError(t *testing.T) {
	querys := []string{
		"select * from A limit 1.3",
		"select A.a,B.b from A join B on A.id=B.id order by A.a limit 's'",
	}
	wants := []string{
		"unsupported: limit.offset.or.counts.must.be.IntVal",
		"unsupported: limit.offset.or.counts.must.be.IntVal",
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

		err = p.pushLimit(sel)
		got := err.Error()
		assert.Equal(t, wants[i], got)
	}
}
func TestPushMisc(t *testing.T) {
	querys := []string{
		"select /* comments */ *  from A for update",
		"select /* comments */ *  from A,B where A.id=B.id and A.id>1 for update",
	}
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig())
	assert.Nil(t, err)

	for _, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)

		p, err := scanTableExprs(log, route, database, sel.From)
		assert.Nil(t, err)
		p.pushMisc(sel)
	}
}
