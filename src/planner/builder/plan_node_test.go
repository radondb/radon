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

	scatter, fakedbs, cleanup := backend.MockScatter(log, 10)
	defer cleanup()
	fakedbs.AddQueryPattern("desc .*", descResult)

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

		fields, _, err := parseSelectExprs(scatter, b.root, b.tables, &sel.SelectExprs)
		assert.Nil(t, err)
		switch p := b.root.(type) {
		case *MergeNode:
			p.fields = fields
		case *JoinNode:
			p.fields = fields
		}

		err = b.root.pushOrderBy(sel.OrderBy)
		assert.Nil(t, err)
	}
}

func TestPushOrderByError(t *testing.T) {
	querys := []string{
		"select A.a from A join B on A.id=B.id order by b",
		"select A.a from A join B on A.id=B.id order by C.a",
	}
	wants := []string{
		"unsupported: column.'b'.in.order.clause.is.ambiguous",
		"unsupported: unknow.table.in.order.by.field[C.a]",
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
		switch p := b.root.(type) {
		case *MergeNode:
			p.fields = fields
		case *JoinNode:
			p.fields = fields
		}

		err = b.root.pushOrderBy(sel.OrderBy)
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

		err = b.root.pushLimit(sel.Limit)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(b.root.Children()))
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

		err = b.root.pushLimit(sel.Limit)
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

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig())
	assert.Nil(t, err)

	for _, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)
		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(sel.From)
		assert.Nil(t, err)
		b.root.pushMisc(sel)
	}
}
