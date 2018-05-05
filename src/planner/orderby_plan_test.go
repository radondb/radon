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

func TestOrderByPlan(t *testing.T) {
	querys := []string{
		"select a,b from t order by a",
		"select * from t order by a",
		"select a,*,c,d from t order by a asc",
		"select a as b,c,d from t order by b desc",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	for _, query := range querys {
		tree, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := tree.(*sqlparser.Select)
		tuples, err := parserSelectExprs(node.SelectExprs)
		assert.Nil(t, err)
		plan := NewOrderByPlan(log, node, tuples)
		// plan build
		{
			err := plan.Build()
			assert.Nil(t, err)
			log.Debug("%v,%v,%s", plan.Type(), plan.Children(), plan.JSON())
		}
		log.Debug("\n")
	}
}

func TestOrderByPlanError(t *testing.T) {
	querys := []string{
		"select a,b from t order by c",
		"select a,b from t order by rand()",
	}
	results := []string{
		"unsupported: orderby[c].should.in.select.list",
		"unsupported: orderby:&{Qualifier: Name:rand Distinct:false Exprs:[]}",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	for i, query := range querys {
		tree, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := tree.(*sqlparser.Select)
		tuples, err := parserSelectExprs(node.SelectExprs)
		assert.Nil(t, err)
		plan := NewOrderByPlan(log, node, tuples)
		// plan build
		{
			err := plan.Build()
			want := results[i]
			got := err.Error()
			assert.Equal(t, want, got)
		}
	}
}
