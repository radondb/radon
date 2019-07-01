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

func TestLimitPlan(t *testing.T) {
	querys := []string{
		"select a,b from t order by a limit 10,9",
		"select a,b from t order by a limit 10",
		"select a,b from t",
	}
	results := []string{
		`{
	"Offset": 10,
	"Limit": 9
}`,
		`{
	"Offset": 0,
	"Limit": 10
}`,
		`{
	"Offset": 0,
	"Limit": 0
}`,
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	for i, query := range querys {
		tree, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := tree.(*sqlparser.Select)
		assert.Nil(t, err)
		plan := NewLimitPlan(log, node.Limit)
		// plan build
		{
			err := plan.Build()
			assert.Nil(t, err)
			want := results[i]
			got := plan.JSON()
			assert.Equal(t, want, got)
			assert.True(t, nil == plan.Children())
			assert.Equal(t, PlanTypeLimit, plan.Type())
		}
	}
}

func TestLimitPlanReWritten(t *testing.T) {
	querys := []string{
		"select a,b from t order by a limit 10,9",
		"select a,b from t order by a limit 10",
		"select a,b from t",
	}
	results := []string{
		" limit 19",
		" limit 10",
		"",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	for i, query := range querys {
		tree, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := tree.(*sqlparser.Select)
		assert.Nil(t, err)
		plan := NewLimitPlan(log, node.Limit)
		// plan build
		{
			err := plan.Build()
			assert.Nil(t, err)
			want := results[i]

			buf := sqlparser.NewTrackedBuffer(nil)
			buf.Myprintf("%v", plan.ReWritten())
			got := buf.String()
			assert.Equal(t, want, got)
		}
	}
}

func TestLimitPlanError(t *testing.T) {
	querys := []string{
		"select a,b from t order by a limit 10,x",
		"select a,b from t order by a limit x,1",
		"select a,b from t order by a limit x",
		"select a,b from t order by a limit 3.1415",
	}
	results := []string{
		"unsupported: limit.offset.or.counts.must.be.IntVal",
		"unsupported: limit.offset.or.counts.must.be.IntVal",
		"unsupported: limit.offset.or.counts.must.be.IntVal",
		"unsupported: limit.offset.or.counts.must.be.IntVal",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	for i, query := range querys {
		tree, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := tree.(*sqlparser.Select)
		assert.Nil(t, err)
		plan := NewLimitPlan(log, node.Limit)
		// plan build
		{
			err := plan.Build()
			want := results[i]
			got := err.Error()
			assert.Equal(t, want, got)
		}
	}
}
