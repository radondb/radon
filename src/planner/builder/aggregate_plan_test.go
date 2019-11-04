/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

import (
	"router"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestAggregatePlan(t *testing.T) {
	querys := []string{
		"select 1, a, min(b), max(a), avg(a), sum(a), count(a), b as b1, avg(b), c, avg(c)  from A group by a, b1, c",
	}
	results := []string{
		`{
	"Aggrs": [
		{
			"Field": "min(b)",
			"Index": 2,
			"Type": "MIN",
			"Distinct": false
		},
		{
			"Field": "max(a)",
			"Index": 3,
			"Type": "MAX",
			"Distinct": false
		},
		{
			"Field": "avg(a)",
			"Index": 4,
			"Type": "AVG",
			"Distinct": false
		},
		{
			"Field": "sum(a)",
			"Index": 4,
			"Type": "SUM",
			"Distinct": false
		},
		{
			"Field": "count(a)",
			"Index": 5,
			"Type": "COUNT",
			"Distinct": false
		},
		{
			"Field": "sum(a)",
			"Index": 6,
			"Type": "SUM",
			"Distinct": false
		},
		{
			"Field": "count(a)",
			"Index": 7,
			"Type": "COUNT",
			"Distinct": false
		},
		{
			"Field": "avg(b)",
			"Index": 9,
			"Type": "AVG",
			"Distinct": false
		},
		{
			"Field": "sum(b)",
			"Index": 9,
			"Type": "SUM",
			"Distinct": false
		},
		{
			"Field": "count(b)",
			"Index": 10,
			"Type": "COUNT",
			"Distinct": false
		},
		{
			"Field": "avg(c)",
			"Index": 12,
			"Type": "AVG",
			"Distinct": false
		},
		{
			"Field": "sum(c)",
			"Index": 12,
			"Type": "SUM",
			"Distinct": false
		},
		{
			"Field": "count(c)",
			"Index": 13,
			"Type": "COUNT",
			"Distinct": false
		},
		{
			"Field": "a",
			"Index": 1,
			"Type": "GROUP BY",
			"Distinct": false
		},
		{
			"Field": "b",
			"Index": 8,
			"Type": "GROUP BY",
			"Distinct": false
		},
		{
			"Field": "c",
			"Index": 11,
			"Type": "GROUP BY",
			"Distinct": false
		}
	],
	"ReWritten": "1, a, min(b), max(a), sum(a) as ` + "`avg(a)`" + `, count(a), sum(a), count(a), b as b1, sum(b) as ` + "`avg(b)`" + `, count(b), c, sum(c) as ` + "`avg(c)`" + `, count(c)"
}`,
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	route, cleanup := router.MockNewRouter(log)
	defer cleanup()
	err := route.AddForTest("sbtest", router.MockTableMConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		tree, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := tree.(*sqlparser.Select)
		assert.Nil(t, err)
		p, err := scanTableExprs(log, route, "sbtest", node.From)
		assert.Nil(t, err)
		tuples, aggTyp, err := parseSelectExprs(node.SelectExprs, p)
		assert.Nil(t, err)
		assert.Equal(t, canPush, aggTyp)
		_, ok := p.(*MergeNode)
		groups, err := checkGroupBy(node.GroupBy, tuples, route, p.getReferTables(), ok)
		assert.Nil(t, err)
		plan := NewAggregatePlan(log, node.SelectExprs, tuples, groups, true)
		// plan build
		{
			err := plan.Build()
			assert.Nil(t, err)
			want := results[i]
			got := plan.JSON()
			log.Debug(got)
			assert.Equal(t, want, got)

			assert.Equal(t, 13, len(plan.NormalAggregators()))
			assert.Equal(t, 3, len(plan.GroupAggregators()))
			assert.False(t, plan.Empty())
			assert.Equal(t, ChildTypeAggregate, plan.Type())
		}
	}
}

// TestAggregatePlanUpperCase test Aggregate func in uppercase
func TestAggregatePlanUpperCase(t *testing.T) {
	querys := []string{
		"select 1, a, MIN(b), MAX(a), AVG(a), SUM(a), COUNT(a), b as b1, AVG(b), c, AVG(c)  from A group by a, b1, c",
	}
	results := []string{
		`{
	"Aggrs": [
		{
			"Field": "MIN(b)",
			"Index": 2,
			"Type": "MIN",
			"Distinct": false
		},
		{
			"Field": "MAX(a)",
			"Index": 3,
			"Type": "MAX",
			"Distinct": false
		},
		{
			"Field": "AVG(a)",
			"Index": 4,
			"Type": "AVG",
			"Distinct": false
		},
		{
			"Field": "sum(a)",
			"Index": 4,
			"Type": "SUM",
			"Distinct": false
		},
		{
			"Field": "count(a)",
			"Index": 5,
			"Type": "COUNT",
			"Distinct": false
		},
		{
			"Field": "SUM(a)",
			"Index": 6,
			"Type": "SUM",
			"Distinct": false
		},
		{
			"Field": "COUNT(a)",
			"Index": 7,
			"Type": "COUNT",
			"Distinct": false
		},
		{
			"Field": "AVG(b)",
			"Index": 9,
			"Type": "AVG",
			"Distinct": false
		},
		{
			"Field": "sum(b)",
			"Index": 9,
			"Type": "SUM",
			"Distinct": false
		},
		{
			"Field": "count(b)",
			"Index": 10,
			"Type": "COUNT",
			"Distinct": false
		},
		{
			"Field": "AVG(c)",
			"Index": 12,
			"Type": "AVG",
			"Distinct": false
		},
		{
			"Field": "sum(c)",
			"Index": 12,
			"Type": "SUM",
			"Distinct": false
		},
		{
			"Field": "count(c)",
			"Index": 13,
			"Type": "COUNT",
			"Distinct": false
		},
		{
			"Field": "a",
			"Index": 1,
			"Type": "GROUP BY",
			"Distinct": false
		},
		{
			"Field": "b",
			"Index": 8,
			"Type": "GROUP BY",
			"Distinct": false
		},
		{
			"Field": "c",
			"Index": 11,
			"Type": "GROUP BY",
			"Distinct": false
		}
	],
	"ReWritten": "1, a, MIN(b), MAX(a), sum(a) as ` + "`AVG(a)`" + `, count(a), SUM(a), COUNT(a), b as b1, sum(b) as ` + "`AVG(b)`" + `, count(b), c, sum(c) as ` + "`AVG(c)`" + `, count(c)"
}`,
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	route, cleanup := router.MockNewRouter(log)
	defer cleanup()
	err := route.AddForTest("sbtest", router.MockTableMConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		tree, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := tree.(*sqlparser.Select)
		assert.Nil(t, err)
		p, err := scanTableExprs(log, route, "sbtest", node.From)
		assert.Nil(t, err)
		tuples, aggTyp, err := parseSelectExprs(node.SelectExprs, p)
		assert.Nil(t, err)
		assert.Equal(t, canPush, aggTyp)
		_, ok := p.(*MergeNode)
		groups, err := checkGroupBy(node.GroupBy, tuples, route, p.getReferTables(), ok)
		assert.Nil(t, err)
		plan := NewAggregatePlan(log, node.SelectExprs, tuples, groups, true)
		// plan build
		{
			err := plan.Build()
			assert.Nil(t, err)
			want := results[i]
			got := plan.JSON()
			log.Debug(got)
			assert.Equal(t, want, got)

			assert.Equal(t, 13, len(plan.NormalAggregators()))
			assert.Equal(t, 3, len(plan.GroupAggregators()))
			assert.False(t, plan.Empty())
		}
	}
}

func TestAggregatePlanHaving(t *testing.T) {
	querys := []string{
		"select age,count(*) from A group by age having a >=2",
	}
	results := []string{
		`{
	"Aggrs": [
		{
			"Field": "count(*)",
			"Index": 1,
			"Type": "COUNT",
			"Distinct": false
		},
		{
			"Field": "age",
			"Index": 0,
			"Type": "GROUP BY",
			"Distinct": false
		}
	],
	"ReWritten": "age, count(*)"
}`,
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	route, cleanup := router.MockNewRouter(log)
	defer cleanup()
	err := route.AddForTest("sbtest", router.MockTableMConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		tree, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := tree.(*sqlparser.Select)
		assert.Nil(t, err)
		p, err := scanTableExprs(log, route, "sbtest", node.From)
		assert.Nil(t, err)
		tuples, aggTyp, err := parseSelectExprs(node.SelectExprs, p)
		assert.Nil(t, err)
		assert.Equal(t, canPush, aggTyp)
		_, ok := p.(*MergeNode)
		groups, err := checkGroupBy(node.GroupBy, tuples, route, p.getReferTables(), ok)
		assert.Nil(t, err)
		plan := NewAggregatePlan(log, node.SelectExprs, tuples, groups, true)
		// plan build
		{
			err := plan.Build()
			assert.Nil(t, err)
			want := results[i]
			got := plan.JSON()
			log.Debug(got)
			assert.Equal(t, want, got)

			assert.Equal(t, 1, len(plan.NormalAggregators()))
			assert.Equal(t, 1, len(plan.GroupAggregators()))
			assert.False(t, plan.Empty())
		}
	}
}

func TestAggregatePlanUnsupported(t *testing.T) {
	querys := []string{
		"select sum(a)  from A group by d",
		"select sum(a),d  from A group by db.t.d",
	}
	results := []string{
		"unsupported: group.by.field[d].should.be.in.select.list",
		"unsupported: unknow.table.in.group.by.field[t.d]",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	route, cleanup := router.MockNewRouter(log)
	defer cleanup()
	err := route.AddForTest("sbtest", router.MockTableMConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		tree, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := tree.(*sqlparser.Select)
		p, err := scanTableExprs(log, route, "sbtest", node.From)
		assert.Nil(t, err)
		tuples, aggTyp, err := parseSelectExprs(node.SelectExprs, p)
		assert.Nil(t, err)
		assert.Equal(t, canPush, aggTyp)
		_, ok := p.(*MergeNode)
		groups, err := checkGroupBy(node.GroupBy, tuples, route, p.getReferTables(), ok)
		if err == nil {
			plan := NewAggregatePlan(log, node.SelectExprs, tuples, groups, aggTyp != notPush)
			err := plan.Build()
			got := err.Error()
			assert.Equal(t, results[i], got)
		} else {
			got := err.Error()
			assert.Equal(t, results[i], got)
		}
	}
}

func TestAggregatePlans(t *testing.T) {
	querys := []string{
		"select avg(a) as b1, avg(c*100)  from A",
		"select avg(distinct b),count(*) from A",
	}
	results := []string{
		`{
	"Aggrs": [
		{
			"Field": "avg(a)",
			"Index": 0,
			"Type": "AVG",
			"Distinct": false
		},
		{
			"Field": "sum(a)",
			"Index": 0,
			"Type": "SUM",
			"Distinct": false
		},
		{
			"Field": "count(a)",
			"Index": 1,
			"Type": "COUNT",
			"Distinct": false
		},
		{
			"Field": "avg(c * 100)",
			"Index": 2,
			"Type": "AVG",
			"Distinct": false
		},
		{
			"Field": "sum(c * 100)",
			"Index": 2,
			"Type": "SUM",
			"Distinct": false
		},
		{
			"Field": "count(c * 100)",
			"Index": 3,
			"Type": "COUNT",
			"Distinct": false
		}
	],
	"ReWritten": "sum(a) as b1, count(a), sum(c * 100) as ` + "`avg(c * 100)`" + `, count(c * 100)"
}`,
		`{
	"Aggrs": [
		{
			"Field": "avg(distinct b)",
			"Index": 0,
			"Type": "AVG",
			"Distinct": true
		},
		{
			"Field": "count(*)",
			"Index": 1,
			"Type": "COUNT",
			"Distinct": false
		}
	],
	"ReWritten": "b as ` + "`avg(distinct b)`" + `, 1 as ` + "`count(*)`" + `"
}`,
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	route, cleanup := router.MockNewRouter(log)
	defer cleanup()
	err := route.AddForTest("sbtest", router.MockTableMConfig())
	assert.Nil(t, err)
	for i, query := range querys {
		tree, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := tree.(*sqlparser.Select)
		assert.Nil(t, err)
		p, err := scanTableExprs(log, route, "sbtest", node.From)
		assert.Nil(t, err)
		tuples, aggTyp, err := parseSelectExprs(node.SelectExprs, p)
		assert.Nil(t, err)
		//assert.Equal(t, canPush, aggTyp)
		_, ok := p.(*MergeNode)
		groups, err := checkGroupBy(node.GroupBy, tuples, route, p.getReferTables(), ok)
		assert.Nil(t, err)
		plan := NewAggregatePlan(log, node.SelectExprs, tuples, groups, aggTyp != notPush)
		// plan build
		{
			err := plan.Build()
			assert.Nil(t, err)
			want := results[i]
			got := plan.JSON()
			log.Debug(got)
			assert.Equal(t, want, got)

			assert.False(t, plan.Empty())
		}
	}
}
