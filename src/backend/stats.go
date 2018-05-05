/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package backend

import (
	"time"
	"xbase/stats"
)

var (
	// mysqlStats shows the time histogram for operations spent on mysql side.
	mysqlStats = stats.NewTimings("MySQL")

	// queryStats shows the time histogram for each type of queries.
	queryStats = stats.NewTimings("Query")

	// queryRates shows the qps of QueryStats. Sample every 5 seconds and keep samples for 1.
	queryRates = stats.NewRates("QPS", queryStats, 1, 5*time.Second)

	// for transactions.
	txnCounters = stats.NewCounters("TxnCounters")

	tz = NewTxnz()
	qz = NewQueryz()
)

// Queryz returns the queryz.
func (scatter *Scatter) Queryz() *Queryz {
	return qz
}

// Txnz returns the txnz.
func (scatter *Scatter) Txnz() *Txnz {
	return tz
}

// MySQLStats returns the mysql stats.
func (scatter *Scatter) MySQLStats() *stats.Timings {
	return mysqlStats
}

// QueryStats returns the query stats.
func (scatter *Scatter) QueryStats() *stats.Timings {
	return queryStats
}

// QueryRates returns the query rates.
func (scatter *Scatter) QueryRates() *stats.Rates {
	return queryRates
}

// TxnCounters returns the txn counters.
func (scatter *Scatter) TxnCounters() *stats.Counters {
	return txnCounters
}
