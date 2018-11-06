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

func TestJoinPlan(t *testing.T) {
	querys := []string{
		"select * from t where t.a=t.b",
		"select x.id, y.id from x join y on x.id=y.id where x.id=1",
		"select x.id, y.id from x,y where x.id=y.id",
	}

	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	for _, query := range querys {
		tree, err := sqlparser.Parse(query)
		assert.Nil(t, err)
		node := tree.(*sqlparser.Select)
		assert.Nil(t, err)
		plan := NewJoinPlan(log, node)
		{
			err := plan.Build()
			assert.Nil(t, err)
			assert.Nil(t, plan.Children())
			assert.Equal(t, "", plan.JSON())
		}
	}
}
