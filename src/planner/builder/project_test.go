/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
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

func TestParserSelectExprsSubquery(t *testing.T) {
	query := "select A.*,(select b.str from b where A.id=B.id) str from A"
	want := "unsupported: subqueries.in.select.exprs"

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	scatter, fakedbs, cleanup := backend.MockScatter(log, 10)
	defer cleanup()
	fakedbs.AddQueryPattern("desc .*", descResult)

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()
	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig())
	assert.Nil(t, err)
	b := NewPlanBuilder(log, route, scatter, "sbtest")
	node, err := sqlparser.Parse(query)
	assert.Nil(t, err)

	sel := node.(*sqlparser.Select)
	b.root, err = b.scanTableExprs(sel.From)
	assert.Nil(t, err)
	_, _, err = parseSelectExprs(scatter, b.root, b.tables, &sel.SelectExprs)
	got := err.Error()
	assert.Equal(t, want, got)
}

func TestGetSelectExprs(t *testing.T) {
	querys := []string{
		"select a,b from A",
		"select a,b from A union select b,a from B",
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
		getSelectExprs(node.(sqlparser.SelectStatement))
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
		"select A.id as tmp from A group by TMP",
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
		0,
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

		fields, _, err := parseSelectExprs(nil, b.root, b.tables, &sel.SelectExprs)
		assert.Nil(t, err)

		_, ok := b.root.(*MergeNode)
		groups, err := b.checkGroupBy(sel.GroupBy, fields, ok)
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

		fields, _, err := parseSelectExprs(nil, b.root, b.tables, &sel.SelectExprs)
		assert.Nil(t, err)

		_, ok := b.root.(*MergeNode)
		_, err = b.checkGroupBy(sel.GroupBy, fields, ok)
		got := err.Error()
		assert.Equal(t, wants[i], got)
	}
}

func TestCheckDistinct(t *testing.T) {
	querys := []string{
		"select distinct A.a,A.b as c from A",
		"select distinct A.id from A",
		"select distinct A.a,A.b,A.c from A group by a",
		"select distinct * from A",
	}
	wants := []int{
		2,
		1,
		1,
		2,
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	scatter, fakedbs, cleanup := backend.MockScatter(log, 10)
	defer cleanup()
	fakedbs.AddQueryPattern("desc .*", descResult)

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)

	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)

		b := NewPlanBuilder(log, route, scatter, "sbtest")
		b.root, err = b.scanTableExprs(sel.From)
		assert.Nil(t, err)

		fields, _, err := parseSelectExprs(scatter, b.root, b.tables, &sel.SelectExprs)
		assert.Nil(t, err)

		_, ok := b.root.(*MergeNode)
		_, err = b.checkDistinct(sel, nil, fields, ok)
		assert.Nil(t, err)
		assert.Equal(t, wants[i], len(sel.GroupBy))
	}
}

func TestCheckDistinctError(t *testing.T) {
	querys := []string{
		"select distinct A.a+1 as a, A.b*10 from A",
	}
	wants := []string{
		"unsupported: distinct",
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

	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)

		b := NewPlanBuilder(log, route, scatter, "sbtest")
		b.root, err = b.scanTableExprs(sel.From)
		assert.Nil(t, err)

		fields, _, err := parseSelectExprs(scatter, b.root, b.tables, &sel.SelectExprs)
		assert.Nil(t, err)

		_, ok := b.root.(*MergeNode)
		_, err = b.checkDistinct(sel, nil, fields, ok)
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

		fields, aggTyp, err := parseSelectExprs(nil, b.root, b.tables, &sel.SelectExprs)
		assert.Nil(t, err)

		_, ok := b.root.(*MergeNode)
		groups, err := b.checkGroupBy(sel.GroupBy, fields, ok)
		assert.Nil(t, err)

		err = b.root.pushSelectExprs(fields, groups, sel, aggTyp)
		assert.Nil(t, err)

		err = b.root.pushOrderBy(sel.OrderBy)
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

		_, ok := b.root.(*MergeNode)
		groups, err := b.checkGroupBy(sel.GroupBy, fields, ok)
		assert.Nil(t, err)

		err = b.root.pushSelectExprs(fields, groups, sel, aggTyp)
		got := err.Error()
		assert.Equal(t, wants[i], got)
	}
}
