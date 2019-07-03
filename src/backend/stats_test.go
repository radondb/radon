/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package backend

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestStats(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	scatter := NewScatter(log, "", 0)
	// Others.
	{
		assert.NotNil(t, scatter.Queryz())
		assert.NotNil(t, scatter.Txnz())

		assert.NotNil(t, scatter.MySQLStats())
		log.Debug(scatter.MySQLStats().String())

		assert.NotNil(t, scatter.QueryStats())
		log.Debug(scatter.QueryStats().String())

		assert.NotNil(t, scatter.QueryRates())
		log.Debug(scatter.QueryRates().String())

		assert.NotNil(t, scatter.TxnCounters())
		log.Debug(scatter.TxnCounters().String())
	}
}
