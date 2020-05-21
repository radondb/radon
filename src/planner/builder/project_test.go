/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
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

func TestParserSelectExprsSubquery(t *testing.T) {
	query := "select A.*,(select b.str from b where A.id=B.id) str from A"
	want := "unsupported: subqueries.in.select.exprs"

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"
	route, cleanup := router.MockNewRouter(log)
	defer cleanup()
	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig())
	assert.Nil(t, err)

	node, err := sqlparser.Parse(query)
	assert.Nil(t, err)

	sel := node.(*sqlparser.Select)
	p, err := scanTableExprs(log, route, database, sel.From)
	assert.Nil(t, err)
	_, _, err = parseSelectExprs(sel.SelectExprs, p)
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

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)

	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)

		p, err := scanTableExprs(log, route, database, sel.From)
		assert.Nil(t, err)

		fields, _, err := parseSelectExprs(sel.SelectExprs, p)
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

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)

	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)

		p, err := scanTableExprs(log, route, database, sel.From)
		assert.Nil(t, err)

		fields, _, err := parseSelectExprs(sel.SelectExprs, p)
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

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)

	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)

		p, err := scanTableExprs(log, route, database, sel.From)
		assert.Nil(t, err)

		fields, _, err := parseSelectExprs(sel.SelectExprs, p)
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

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)

	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)

		p, err := scanTableExprs(log, route, database, sel.From)
		assert.Nil(t, err)

		fields, _, err := parseSelectExprs(sel.SelectExprs, p)
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

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)

	for _, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		sel := node.(*sqlparser.Select)

		p, err := scanTableExprs(log, route, database, sel.From)
		assert.Nil(t, err)

		fields, aggTyp, err := parseSelectExprs(sel.SelectExprs, p)
		assert.Nil(t, err)

		_, ok := p.(*MergeNode)
		groups, err := checkGroupBy(sel.GroupBy, fields, route, p.getReferTables(), ok)
		assert.Nil(t, err)

		err = p.pushSelectExprs(fields, groups, sel, aggTyp)
		assert.Nil(t, err)

		err = p.pushOrderBy(sel.OrderBy)
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

		p, err := scanTableExprs(log, route, database, sel.From)
		assert.Nil(t, err)

		fields, aggTyp, err := parseSelectExprs(sel.SelectExprs, p)
		assert.Nil(t, err)

		_, ok := p.(*MergeNode)
		groups, err := checkGroupBy(sel.GroupBy, fields, route, p.getReferTables(), ok)
		assert.Nil(t, err)

		err = p.pushSelectExprs(fields, groups, sel, aggTyp)
		got := err.Error()
		assert.Equal(t, wants[i], got)
	}
}

func TestReplaceSelect(t *testing.T) {
	query := "select tmp+a,cnt,b from (select a+1 as tmp,sum(b) as cnt,a from A) t"
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig())
	assert.Nil(t, err)

	node, err := sqlparser.Parse(query)
	assert.Nil(t, err)
	sel := node.(*sqlparser.Select)

	p, err := BuildNode(log, route, database, sel.From[0].(*sqlparser.AliasedTableExpr).Expr.(*sqlparser.Subquery).Select)
	assert.Nil(t, err)

	colMap := make(map[string]selectTuple)
	for _, field := range p.getFields() {
		name := field.alias
		if name == "" {
			name = field.field
		}
		colMap[name] = field
	}

	{
		tuple := parseExpr(sel.SelectExprs[0].(*sqlparser.AliasedExpr).Expr)
		field, err := replaceSelect(tuple, colMap)
		assert.Equal(t, "tmp + a", field.alias)

		buf := sqlparser.NewTrackedBuffer(nil)
		field.expr.Format(buf)
		assert.Nil(t, err)
		assert.Equal(t, "a + 1 + a as `tmp + a`", buf.String())
	}
	{
		tuple := parseExpr(sel.SelectExprs[1].(*sqlparser.AliasedExpr).Expr)
		_, err = replaceSelect(tuple, colMap)
		assert.NotNil(t, err)
		assert.Equal(t, "unsupported: aggregation.field.in.subquery.is.used.in.clause", err.Error())
	}
	{
		tuple := parseExpr(sel.SelectExprs[2].(*sqlparser.AliasedExpr).Expr)
		_, err = replaceSelect(tuple, colMap)
		assert.NotNil(t, err)
		assert.Equal(t, "unsupported: unknown.column.name.'b'", err.Error())
	}
}
