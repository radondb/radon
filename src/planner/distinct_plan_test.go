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

func TestDistinctPlan(t *testing.T) {
	querys := []string{
		"select distinct(a), b from t",
	}
	results := []string{
		"unsupported: distinct",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	for i, query := range querys {
		tree, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := tree.(*sqlparser.Select)
		assert.Nil(t, err)
		plan := NewDistinctPlan(log, node)
		{
			err := plan.Build()
			want := results[i]
			got := err.Error()
			assert.Equal(t, want, got)

			assert.Nil(t, plan.Children())
			assert.Equal(t, "", plan.JSON())
		}
	}
}
