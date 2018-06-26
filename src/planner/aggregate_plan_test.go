/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package planner

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestAggregatePlan(t *testing.T) {
	querys := []string{
		"select 1, a, min(b), max(a), avg(a), sum(a), count(a), b as b1, avg(b), c, avg(c)  from t group by a, b1, c",
	}
	results := []string{
		`{
	"Aggrs": [
		{
			"Field": "min(b)",
			"Index": 2,
			"Type": "MIN"
		},
		{
			"Field": "max(a)",
			"Index": 3,
			"Type": "MAX"
		},
		{
			"Field": "avg(a)",
			"Index": 4,
			"Type": "AVG"
		},
		{
			"Field": "sum(a)",
			"Index": 5,
			"Type": "SUM"
		},
		{
			"Field": "count(a)",
			"Index": 6,
			"Type": "COUNT"
		},
		{
			"Field": "sum(a)",
			"Index": 7,
			"Type": "SUM"
		},
		{
			"Field": "count(a)",
			"Index": 8,
			"Type": "COUNT"
		},
		{
			"Field": "avg(b)",
			"Index": 10,
			"Type": "AVG"
		},
		{
			"Field": "sum(b)",
			"Index": 11,
			"Type": "SUM"
		},
		{
			"Field": "count(b)",
			"Index": 12,
			"Type": "COUNT"
		},
		{
			"Field": "avg(c)",
			"Index": 14,
			"Type": "AVG"
		},
		{
			"Field": "sum(c)",
			"Index": 15,
			"Type": "SUM"
		},
		{
			"Field": "count(c)",
			"Index": 16,
			"Type": "COUNT"
		},
		{
			"Field": "a",
			"Index": 1,
			"Type": "GROUP BY"
		},
		{
			"Field": "b1",
			"Index": 9,
			"Type": "GROUP BY"
		},
		{
			"Field": "c",
			"Index": 13,
			"Type": "GROUP BY"
		}
	],
	"ReWritten": "1, a, min(b), max(a), avg(a), sum(a), count(a), sum(a), count(a), b as b1, avg(b), sum(b), count(b), c, avg(c), sum(c), count(c)"
}`,
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	for i, query := range querys {
		tree, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := tree.(*sqlparser.Select)
		assert.Nil(t, err)
		tuples, err := parserSelectExprs(node.SelectExprs)
		assert.Nil(t, err)
		plan := NewAggregatePlan(log, node, tuples)
		// plan build
		{
			err := plan.Build()
			assert.Nil(t, err)
			want := results[i]
			got := plan.JSON()
			log.Debug(got)
			assert.Equal(t, want, got)
			assert.True(t, nil == plan.Children())
			assert.Equal(t, 13, len(plan.NormalAggregators()))
			assert.Equal(t, 3, len(plan.GroupAggregators()))
			assert.False(t, plan.Empty())
		}
	}
}

// TestAggregatePlanUpperCase test Aggregate func in uppercase
func TestAggregatePlanUpperCase(t *testing.T) {
	querys := []string{
		"select 1, a, MIN(b), MAX(a), AVG(a), SUM(a), COUNT(a), b as b1, AVG(b), c, AVG(c)  from t group by a, b1, c",
	}
	results := []string{
		`{
	"Aggrs": [
		{
			"Field": "MIN(b)",
			"Index": 2,
			"Type": "MIN"
		},
		{
			"Field": "MAX(a)",
			"Index": 3,
			"Type": "MAX"
		},
		{
			"Field": "AVG(a)",
			"Index": 4,
			"Type": "AVG"
		},
		{
			"Field": "sum(a)",
			"Index": 5,
			"Type": "SUM"
		},
		{
			"Field": "count(a)",
			"Index": 6,
			"Type": "COUNT"
		},
		{
			"Field": "SUM(a)",
			"Index": 7,
			"Type": "SUM"
		},
		{
			"Field": "COUNT(a)",
			"Index": 8,
			"Type": "COUNT"
		},
		{
			"Field": "AVG(b)",
			"Index": 10,
			"Type": "AVG"
		},
		{
			"Field": "sum(b)",
			"Index": 11,
			"Type": "SUM"
		},
		{
			"Field": "count(b)",
			"Index": 12,
			"Type": "COUNT"
		},
		{
			"Field": "AVG(c)",
			"Index": 14,
			"Type": "AVG"
		},
		{
			"Field": "sum(c)",
			"Index": 15,
			"Type": "SUM"
		},
		{
			"Field": "count(c)",
			"Index": 16,
			"Type": "COUNT"
		},
		{
			"Field": "a",
			"Index": 1,
			"Type": "GROUP BY"
		},
		{
			"Field": "b1",
			"Index": 9,
			"Type": "GROUP BY"
		},
		{
			"Field": "c",
			"Index": 13,
			"Type": "GROUP BY"
		}
	],
	"ReWritten": "1, a, MIN(b), MAX(a), AVG(a), sum(a), count(a), SUM(a), COUNT(a), b as b1, AVG(b), sum(b), count(b), c, AVG(c), sum(c), count(c)"
}`,
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	for i, query := range querys {
		tree, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := tree.(*sqlparser.Select)
		assert.Nil(t, err)
		tuples, err := parserSelectExprs(node.SelectExprs)
		assert.Nil(t, err)
		plan := NewAggregatePlan(log, node, tuples)
		// plan build
		{
			err := plan.Build()
			assert.Nil(t, err)
			want := results[i]
			got := plan.JSON()
			log.Debug(got)
			assert.Equal(t, want, got)
			assert.True(t, nil == plan.Children())
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
			"Type": "COUNT"
		},
		{
			"Field": "age",
			"Index": 0,
			"Type": "GROUP BY"
		}
	],
	"ReWritten": "age, count(*)"
}`,
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	for i, query := range querys {
		tree, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := tree.(*sqlparser.Select)
		assert.Nil(t, err)
		tuples, err := parserSelectExprs(node.SelectExprs)
		assert.Nil(t, err)
		plan := NewAggregatePlan(log, node, tuples)
		// plan build
		{
			err := plan.Build()
			assert.Nil(t, err)
			want := results[i]
			got := plan.JSON()
			log.Debug(got)
			assert.Equal(t, want, got)
			assert.True(t, nil == plan.Children())
			assert.Equal(t, 1, len(plan.NormalAggregators()))
			assert.Equal(t, 1, len(plan.GroupAggregators()))
			assert.False(t, plan.Empty())
		}
	}
}

func TestAggregatePlanUnsupported(t *testing.T) {
	querys := []string{
		"select sum(a)  from t group by d",
		"select sum(a),d  from t group by db.t.d",
		"select rand(a),d  from t group by a",
		"select count(distinct b) from t",
		"select age,count(*) from A group by age having count(*) >=2",
	}
	results := []string{
		"unsupported: group.by.field[d].should.be.in.select.list",
		"unsupported: group.by.field[d].have.table.name[t].please.use.AS.keyword",
		"unsupported: function:rand",
		"unsupported: distinct.in.function:count",
		"unsupported: expr[count(*)].in.having.clause",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	for i, query := range querys {
		tree, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := tree.(*sqlparser.Select)
		tuples, err := parserSelectExprs(node.SelectExprs)
		assert.Nil(t, err)
		plan := NewAggregatePlan(log, node, tuples)
		// plan build
		{
			err := plan.Build()

			want := results[i]
			got := err.Error()
			assert.Equal(t, want, got)
		}
	}
}
