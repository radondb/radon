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

func TestScanTableExprs(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(), router.MockTableCConfig(), router.MockTableGConfig())
	assert.Nil(t, err)
	// single table.
	{
		query := "select * from A"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		m, ok := b.root.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}

		tbMaps := m.getReferTables()
		tbInfo := tbMaps["A"]
		assert.Equal(t, 1, len(tbMaps))
		assert.Equal(t, m, tbInfo.parent)
	}
	// can merge shard tables.
	{
		query := "select * from B join C on B.id=C.a"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		m, ok := b.root.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}

		tbMaps := m.getReferTables()
		tbInfo := tbMaps["B"]
		assert.Equal(t, 2, len(tbMaps))
		assert.Equal(t, m, tbInfo.parent)
	}
	// cannot merge shard tables.
	{
		query := "select * from A join B on B.id=A.id and 1=A.id"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		j, ok := b.root.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 1, len(j.joinOn))
		assert.False(t, j.IsLeftJoin)

		tbMaps := j.getReferTables()
		tbInfo := tbMaps["A"]

		m, ok := j.Left.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, m, tbInfo.parent)
		assert.Equal(t, []int{2323}, m.indexes)
	}
	// left join.
	{
		query := "select * from A left join B on A.id=B.id and A.id=1 and 1=1 and B.a=1 and A.b+B.b>0"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		j, ok := b.root.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 1, len(j.joinOn))
		assert.True(t, j.IsLeftJoin)

		tbMaps := j.getReferTables()
		tbInfo := tbMaps["A"]

		m, ok := j.Left.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, m, tbInfo.parent)
		assert.Equal(t, 0, len(m.indexes))
		assert.NotNil(t, j.otherLeftJoin)

		err = j.pushOthers()
		assert.Nil(t, err)
	}
	// right join1.
	{
		query := "select * from A join B on A.id=B.id right join G on G.id=A.id and A.id=1 and 1=1 and G.a=1"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		j, ok := b.root.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 1, len(j.joinOn))
		assert.True(t, j.IsLeftJoin)
		assert.NotNil(t, j.otherLeftJoin)

		err = j.pushOthers()
		assert.Nil(t, err)
	}
	// right join2.
	{
		query := "select * from A join G on A.id=G.id right join B on A.id=B.id and A.id=1 and 1=1"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		j, ok := b.root.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 1, len(j.joinOn))
		assert.True(t, j.IsLeftJoin)

		tbMaps := j.getReferTables()
		tbInfo := tbMaps["A"]

		m, ok := j.Right.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, m, tbInfo.parent)
		assert.Equal(t, 2, len(m.getReferTables()))
		assert.Equal(t, 0, len(m.indexes))
		assert.True(t, m.hasParen)
		assert.NotNil(t, j.otherLeftJoin)

		err = j.pushOthers()
		assert.Nil(t, err)
	}
	// can merge shard tables.
	{
		query := "select * from A join A as B on A.id=B.id and A.id=1"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		m, ok := b.root.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		tbMaps := m.getReferTables()
		assert.Equal(t, 2, len(tbMaps))
		tbInfo := tbMaps["A"]
		assert.Equal(t, m, tbInfo.parent)
		assert.Equal(t, []int{2323}, m.indexes)
		assert.NotNil(t, m.Sel.(*sqlparser.Select).Where)
	}
	// with parenthese query.
	{
		query := "select * from G,(A, B)"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		j, ok := b.root.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 3, len(j.getReferTables()))
		assert.False(t, j.hasParen)

		j2, ok := j.Right.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 2, len(j2.getReferTables()))
		assert.True(t, j2.hasParen)
	}
	// can merge shard tables and global table.
	{
		query := "select * from B join B as A on A.id=B.id join G on G.id=B.id and B.id=1"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		m, ok := b.root.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		tbMaps := m.getReferTables()
		assert.Equal(t, 3, len(tbMaps))
		tbInfo := tbMaps["B"]
		assert.Equal(t, m, tbInfo.parent)
		assert.Equal(t, []int{2323}, m.indexes)
		assert.NotNil(t, m.Sel.(*sqlparser.Select).Where)
	}
	// two join on conditions.
	{
		query := "select * from A join B on B.a=A.id and A.a=B.id and A.id=1 and 1=1"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		j, ok := b.root.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 2, len(j.joinOn))
		assert.False(t, j.IsLeftJoin)
		assert.Equal(t, 1, len(j.noTableFilter))
		tbMaps := j.getReferTables()
		tbInfo := tbMaps["A"]

		m, ok := j.Left.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, m, tbInfo.parent)
		assert.Equal(t, []int{2323}, m.indexes)
	}
	// without on conditions.
	{
		query := "select * from G join A on A.id=1"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		m, ok := b.root.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		tbMaps := m.getReferTables()
		assert.Equal(t, 2, len(tbMaps))
		assert.Equal(t, []int{2323}, m.indexes)
		assert.Equal(t, 2, len(m.Sel.(*sqlparser.Select).From))
		assert.NotNil(t, m.Sel.(*sqlparser.Select).Where)
	}
	// haspare.
	{
		query := "select * from G, (A join A as B on A.id=B.id and A.id=1)"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		m, ok := b.root.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		tbMaps := m.getReferTables()
		assert.Equal(t, 3, len(tbMaps))
		tbInfo := tbMaps["B"]
		assert.Equal(t, m, tbInfo.parent)
	}
	// haspare.
	{
		query := "select * from (A,G) join A as B on A.id=B.id and G.id=1"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		m, ok := b.root.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		tbMaps := m.getReferTables()
		assert.Equal(t, 3, len(tbMaps))
		tbInfo := tbMaps["B"]
		assert.Equal(t, m, tbInfo.parent)
	}
	// joinnode.
	{
		query := "select * from A join B on A.id=B.a and B.id=A.a join G on G.id= A.id and G.a>A.a"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		j, ok := b.root.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 1, len(j.otherFilter))
		assert.Equal(t, 1, len(j.joinOn))
		tbMaps := j.getReferTables()
		assert.Equal(t, 3, len(tbMaps))
		tbInfo := tbMaps["B"]

		j2, ok := j.Left.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 2, len(j2.joinOn))
		assert.Equal(t, j2.Right, tbInfo.parent)
	}
}

func TestScanTableExprsList(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(),
		router.MockTableCConfig(), router.MockTableGConfig(), router.MockTableListConfig())
	assert.Nil(t, err)
	// list table.
	{
		query := "select * from L"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		m, ok := b.root.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}

		tbMaps := m.getReferTables()
		tbInfo := tbMaps["L"]
		assert.Equal(t, 1, len(tbMaps))
		assert.Equal(t, m, tbInfo.parent)
	}
	// cannot merge shard tables.
	{
		query := "select * from A join L on A.id=L.id and 1=A.id"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		j, ok := b.root.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 1, len(j.joinOn))
		assert.False(t, j.IsLeftJoin)

		tbMaps := j.getReferTables()
		tbInfo := tbMaps["A"]

		m, ok := j.Left.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, m, tbInfo.parent)
		assert.Equal(t, []int{2323}, m.indexes)
	}
	// left join.
	{
		query := "select * from A left join L on A.id=L.id and A.id=1 and 1=1 and L.a=1 and A.b+L.b>0"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		j, ok := b.root.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 1, len(j.joinOn))
		assert.True(t, j.IsLeftJoin)

		tbMaps := j.getReferTables()
		tbInfo := tbMaps["A"]

		m, ok := j.Left.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, m, tbInfo.parent)
		assert.Equal(t, 0, len(m.indexes))
		assert.NotNil(t, j.otherLeftJoin)

		err = j.pushOthers()
		assert.Nil(t, err)
	}
	// right join1.
	{
		query := "select * from A join L on A.id=L.id right join G on G.id=A.id and A.id=1 and 1=1 and G.a=1"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		j, ok := b.root.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 1, len(j.joinOn))
		assert.True(t, j.IsLeftJoin)
		assert.NotNil(t, j.otherLeftJoin)

		err = j.pushOthers()
		assert.Nil(t, err)
	}
	// right join2.
	{
		query := "select * from A join G on A.id=G.id right join L on A.id=L.id and A.id=1 and 1=1"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		j, ok := b.root.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 1, len(j.joinOn))
		assert.True(t, j.IsLeftJoin)
		tbMaps := j.getReferTables()
		tbInfo := tbMaps["A"]

		m, ok := j.Right.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, m, tbInfo.parent)
		assert.Equal(t, 2, len(m.getReferTables()))
		assert.Equal(t, 0, len(m.indexes))
		assert.True(t, m.hasParen)
		assert.NotNil(t, j.otherLeftJoin)

		err = j.pushOthers()
		assert.Nil(t, err)
	}
	// can merge shard tables.
	{
		query := "select * from A join A as L on A.id=L.id and A.id=1"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		m, ok := b.root.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		tbMaps := m.getReferTables()
		assert.Equal(t, 2, len(tbMaps))
		tbInfo := tbMaps["A"]
		assert.Equal(t, m, tbInfo.parent)
		assert.Equal(t, []int{2323}, m.indexes)
		assert.NotNil(t, m.Sel.(*sqlparser.Select).Where)
	}
	// can merge shard tables, columns ignore case.
	{
		query := "select * from A join A as L on A.id=L.ID and A.ID=1"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		m, ok := b.root.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		tbMaps := m.getReferTables()
		assert.Equal(t, 2, len(tbMaps))
		tbInfo := tbMaps["A"]
		assert.Equal(t, m, tbInfo.parent)
		assert.Equal(t, []int{2323}, m.indexes)
		assert.NotNil(t, m.Sel.(*sqlparser.Select).Where)
	}
	// with parenthese query.
	{
		query := "select * from G,(A, L)"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		j, ok := b.root.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 3, len(j.getReferTables()))
		assert.False(t, j.hasParen)

		j2, ok := j.Right.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 2, len(j2.getReferTables()))
		assert.True(t, j2.hasParen)
	}
	// two join on conditions.
	{
		//query := "select * from A join B on B.a=A.id and A.a=B.id and A.id=1 and 1=1"
		query := "select * from A join L on L.id=A.id and A.id=L.id and A.id=1 and 1=1"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		j, ok := b.root.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 2, len(j.joinOn))
		assert.False(t, j.IsLeftJoin)
		assert.Equal(t, 1, len(j.noTableFilter))
		tbMaps := j.getReferTables()
		tbInfo := tbMaps["A"]

		m, ok := j.Left.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, m, tbInfo.parent)
		assert.Equal(t, []int{2323}, m.indexes)
	}
	// without on conditions.
	{
		//query := "select * from G join A on A.id=1"
		query := "select L.id from A join L on A.id=1"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		j, ok := b.root.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 0, len(j.joinOn))
		assert.False(t, j.IsLeftJoin)
		assert.Equal(t, 0, len(j.noTableFilter))
		tbMaps := j.getReferTables()
		tbInfo := tbMaps["L"]

		m, ok := j.Right.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, m, tbInfo.parent)
		// TODO(andyli): Optimize List: Now, index nil, push down to all backends.
		assert.Equal(t, 0, len(m.indexes))
	}
	// haspare.
	{
		//query := "select * from G, (A join A as B on A.id=B.id and A.id=1)"
		query := "select * from G, (L join L as B on L.id=B.id and L.id=1)"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		m, ok := b.root.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		tbMaps := m.getReferTables()
		assert.Equal(t, 3, len(tbMaps))
		tbInfo := tbMaps["B"]
		assert.Equal(t, m, tbInfo.parent)
	}
	// haspare.
	{
		query := "select * from (L,G) join L as B on L.id=B.id and G.id=1"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		m, ok := b.root.(*MergeNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		tbMaps := m.getReferTables()
		assert.Equal(t, 3, len(tbMaps))
		tbInfo := tbMaps["B"]
		assert.Equal(t, m, tbInfo.parent)
	}
	// joinnode.
	{
		query := "select * from L join B on L.id=B.a and B.id=L.a join G on G.id= L.id and G.a>L.a"
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		b := NewPlanBuilder(log, route, nil, "sbtest")
		b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		assert.Nil(t, err)

		j, ok := b.root.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 1, len(j.otherFilter))
		assert.Equal(t, 1, len(j.joinOn))
		tbMaps := j.getReferTables()
		assert.Equal(t, 3, len(tbMaps))
		tbInfo := tbMaps["B"]

		j2, ok := j.Left.(*JoinNode)
		if !ok {
			t.Errorf("scanTableExprs returned plannode error")
		}
		assert.Equal(t, 2, len(j2.joinOn))
		assert.Equal(t, j2.Right, tbInfo.parent)
	}
}

func TestScanTableExprsListErrorDebug(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(),
		router.MockTableCConfig(), router.MockTableGConfig(), router.MockTableListConfig())
	assert.Nil(t, err)
	b := NewPlanBuilder(log, route, nil, "sbtest")
	//query := "select * from G join A on A.id=1"
	query := "select L.id from A join L on A.id=1"
	node, err := sqlparser.Parse(query)
	assert.Nil(t, err)

	b.root, err = b.scanTableExprs(node.(*sqlparser.Select).From)
	assert.Nil(t, err)

	j, ok := b.root.(*JoinNode)
	if !ok {
		t.Errorf("scanTableExprs returned plannode error")
	}
	assert.Equal(t, 0, len(j.joinOn))
	assert.False(t, j.IsLeftJoin)
	assert.Equal(t, 0, len(j.noTableFilter))
	tbMaps := j.getReferTables()
	tbInfo := tbMaps["L"]

	m, ok := j.Right.(*MergeNode)
	if !ok {
		t.Errorf("scanTableExprs returned plannode error")
	}
	assert.Equal(t, m, tbInfo.parent)
	// TODO(andyli): Optimize List: Now, index nil, push down to all backends.
	assert.Equal(t, 0, len(m.indexes))
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
		"select * from G join A as G where G.id=1",
		"select * from A join B on A.id=B.id and B.id=0x12",
	}
	wants := []string{
		"Table 'C' doesn't exist (errno 1146) (sqlstate 42S02)",
		"unsupported: subquery.in.select",
		"unsupported: join.type:natural join",
		"unsupported: unknown.column.'id'.in.clause",
		"unsupported: unknown.column.'C.id'.in.clause",
		"unsupported: unknown.column.'C.id'.in.clause",
		"hash.unsupported.key.type:[3]",
		"hash.unsupported.key.type:[3]",
		"Table 'C' doesn't exist (errno 1146) (sqlstate 42S02)",
		"Table 'C' doesn't exist (errno 1146) (sqlstate 42S02)",
		"unsupported: join.on.condition.should.cross.left-right.tables",
		"unsupported: join.on.condition.should.cross.left-right.tables",
		"unsupported: not.unique.table.or.alias:'G'",
		"hash.unsupported.key.type:[3]",
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
		b := NewPlanBuilder(log, route, nil, "sbtest")
		_, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		got := err.Error()
		assert.Equal(t, wants[i], got)
	}
}

func TestScanTableExprsListError(t *testing.T) {
	querys := []string{
		"select * from (select * from L) as L",
		"select * from L natural join B",
		"select * from A join L on A.id=L.id and id=1",
		"select * from A join L on A.id=L.id and C.id=1",
		"select * from A join L on A.id=C.id and A.id=1",
		"select * from L join A on A.id=0x12",
		"select * from A join L on A.id=L.id and A.id=0x12",
		"select * from L,C",
		"select * from L join C on L.id=C.id",
		"select * from L join A on L.id=A.id join B on A.a=L.a",
		"select * from L join (A,B) on L.id=A.id and A.a=B.a",
		"select * from L join A as L where L.id=1",
	}
	wants := []string{
		"unsupported: subquery.in.select",
		"unsupported: join.type:natural join",
		"unsupported: unknown.column.'id'.in.clause",
		"unsupported: unknown.column.'C.id'.in.clause",
		"unsupported: unknown.column.'C.id'.in.clause",
		"hash.unsupported.key.type:[3]",
		"hash.unsupported.key.type:[3]",
		"Table 'C' doesn't exist (errno 1146) (sqlstate 42S02)",
		"Table 'C' doesn't exist (errno 1146) (sqlstate 42S02)",
		"unsupported: join.on.condition.should.cross.left-right.tables",
		"unsupported: join.on.condition.should.cross.left-right.tables",
		"unsupported: not.unique.table.or.alias:'L'",
	}
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	database := "sbtest"

	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	err := route.CreateDatabase(database)
	assert.Nil(t, err)
	err = route.AddForTest(database, router.MockTableMConfig(), router.MockTableBConfig(),
		router.MockTableGConfig(), router.MockTableListConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		node, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		b := NewPlanBuilder(log, route, nil, "sbtest")
		_, err = b.scanTableExprs(node.(*sqlparser.Select).From)
		got := err.Error()
		assert.Equal(t, wants[i], got)
	}
}
