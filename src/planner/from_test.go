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

func TestScanTableExprs(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableGConfig())
	assert.Nil(t, err)
	// single table.
	{
		query := "select * from A"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		planNode, err := scanTableExprs(log, route, database, node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		m, ok := planNode.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}

		tbMaps := m.getReferredTables()
		tbInfo := tbMaps["A"]
		assert.Equal(t, 1, len(tbMaps))
		assert.Equal(t, m, tbInfo.parent)
	}
	// cannot merge shard tables.
	{
		query := "select * from A join B on B.id=A.id and 1=A.id"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		planNode, err := scanTableExprs(log, route, database, node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		j, ok := planNode.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 1, len(j.JoinOn))
		assert.False(t, j.IsLeftJoin)

		tbMaps := j.getReferredTables()
		tbInfo := tbMaps["A"]
		assert.Equal(t, 1, len(tbInfo.whereFilter))

		m, ok := j.Left.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, m, tbInfo.parent)
		assert.Equal(t, 2323, m.index)
	}
	// left join.
	{
		query := "select * from A left join B on A.id=B.id and A.id=1"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		planNode, err := scanTableExprs(log, route, database, node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		j, ok := planNode.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 1, len(j.JoinOn))
		assert.True(t, j.IsLeftJoin)
		assert.Equal(t, 1, len(j.otherJoinOn))

		tbMaps := j.getReferredTables()
		tbInfo := tbMaps["A"]
		assert.Equal(t, 0, len(tbInfo.whereFilter))

		m, ok := j.Left.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, m, tbInfo.parent)
		assert.Equal(t, -1, m.index)
	}
	// right join.
	{
		query := "select * from A join G on A.id=G.id right join B on A.id=B.id and A.id=1"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		planNode, err := scanTableExprs(log, route, database, node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		j, ok := planNode.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 1, len(j.JoinOn))
		assert.True(t, j.IsLeftJoin)
		assert.Equal(t, 1, len(j.otherJoinOn))

		tbMaps := j.getReferredTables()
		tbInfo := tbMaps["A"]
		assert.Equal(t, 0, len(tbInfo.whereFilter))

		m, ok := j.Right.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, m, tbInfo.parent)
		assert.Equal(t, 2, len(m.getReferredTables()))
		assert.Equal(t, -1, m.index)
		assert.True(t, m.hasParen)
	}
	// can merge shard tables.
	{
		query := "select * from A join A as B on A.id=B.id and A.id=1"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		planNode, err := scanTableExprs(log, route, database, node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		m, ok := planNode.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		tbMaps := m.getReferredTables()
		assert.Equal(t, 2, len(tbMaps))
		tbInfo := tbMaps["A"]
		assert.Equal(t, 0, len(tbInfo.whereFilter))
		assert.Equal(t, m, tbInfo.parent)
		assert.Equal(t, 2323, m.index)
		assert.NotNil(t, m.sel.Where)
	}
	// with parenthese query.
	{
		query := "select * from G,(A, B)"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		planNode, err := scanTableExprs(log, route, database, node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		j, ok := planNode.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 3, len(j.getReferredTables()))
		assert.False(t, j.hasParen)

		j2, ok := j.Right.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 2, len(j2.getReferredTables()))
		assert.True(t, j2.hasParen)
	}
	// can merge shard tables and global table.
	{
		query := "select * from B join B as A on A.id=B.id join G on G.id=B.id and B.id=1"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		planNode, err := scanTableExprs(log, route, database, node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		m, ok := planNode.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		tbMaps := m.getReferredTables()
		assert.Equal(t, 3, len(tbMaps))
		tbInfo := tbMaps["B"]
		assert.Equal(t, 0, len(tbInfo.whereFilter))
		assert.Equal(t, m, tbInfo.parent)
		assert.Equal(t, 2323, m.index)
		assert.NotNil(t, m.sel.Where)
	}
	// two join on conditions.
	{
		query := "select * from A join B on B.a=A.id and A.a=B.id and A.id=1 and 1=1"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		planNode, err := scanTableExprs(log, route, database, node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		j, ok := planNode.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 2, len(j.JoinOn))
		assert.False(t, j.IsLeftJoin)
		assert.Equal(t, 1, len(j.noTableFilter))

		tbMaps := j.getReferredTables()
		tbInfo := tbMaps["A"]
		assert.Equal(t, 1, len(tbInfo.whereFilter))

		m, ok := j.Left.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, m, tbInfo.parent)
		assert.Equal(t, 2323, m.index)
	}
	// without on conditions.
	{
		query := "select * from G join A on A.id=1"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		planNode, err := scanTableExprs(log, route, database, node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		m, ok := planNode.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		tbMaps := m.getReferredTables()
		assert.Equal(t, 2, len(tbMaps))
		assert.Equal(t, 2323, m.index)
		assert.Equal(t, 2, len(m.sel.From))
		assert.NotNil(t, m.sel.Where)
	}
	// haspare.
	{
		query := "select * from G, (A join A as B on A.id=B.id and A.id=1)"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		planNode, err := scanTableExprs(log, route, database, node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		m, ok := planNode.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		tbMaps := m.getReferredTables()
		assert.Equal(t, 3, len(tbMaps))
		tbInfo := tbMaps["B"]
		assert.Equal(t, m, tbInfo.parent)
	}
	// haspare.
	{
		query := "select * from (A,G) join A as B on A.id=B.id and G.id=1"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		planNode, err := scanTableExprs(log, route, database, node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		m, ok := planNode.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		tbMaps := m.getReferredTables()
		assert.Equal(t, 3, len(tbMaps))
		tbInfo := tbMaps["B"]
		assert.Equal(t, m, tbInfo.parent)
	}
	// joinnode.
	{
		query := "select * from A join B on A.id=B.a and B.id=A.a join G on G.id= A.id and G.a>A.a"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		planNode, err := scanTableExprs(log, route, database, node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		j, ok := planNode.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 1, len(j.whereFilter))
		assert.Equal(t, 1, len(j.JoinOn))
		tbMaps := j.getReferredTables()
		assert.Equal(t, 3, len(tbMaps))
		tbInfo := tbMaps["B"]

		j2, ok := j.Left.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 2, len(j2.JoinOn))
		assert.Equal(t, j2.Right, tbInfo.parent)
	}
}

func TestScanTableExprsError(t *testing.T) {
	querys := []string{
		"select * from  C where C.id=1",
		"select * from (select * from A) as D",
		"select * from A natural join B",
		"select * from A join B on A.id=B.id and id=1",
		"select * from A join B on A.id=B.id and C.id=1",
		"select * from A join B on A.id=C.id and A.id=1",
		"select * from G join A on A.id=0x12",
		"select * from A join B on A.id=B.id and A.id=0x12",
		"select * from A,C",
		"select * from A join C on A.id=C.id",
		"select * from G join A on G.id=A.id join B on A.a=G.a",
		"select * from G join (A,B) on G.id=A.id and A.a=B.a",
	}
	wants := []string{
		"Table 'C' doesn't exist (errno 1146) (sqlstate 42S02)",
		"unsupported: subquery.in.select",
		"unsupported: join.type:natural join",
		"unsupported: unknown.column.'id'.in.clause",
		"unsupported: unknown.table.'C'.in.clause",
		"unsupported: unknown.table.'C'.in.clause",
		"hash.unsupported.key.type:[3]",
		"hash.unsupported.key.type:[3]",
		"Table 'C' doesn't exist (errno 1146) (sqlstate 42S02)",
		"Table 'C' doesn't exist (errno 1146) (sqlstate 42S02)",
		"unsupported: join.on.condition.should.cross.left-right.tables",
		"unsupported: join.on.condition.should.cross.left-right.tables",
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
		_, err = scanTableExprs(log, route, database, node.(*sqlparser.Select).From)
		got := err.Error()
		assert.Equal(t, wants[i], got)
	}
}
