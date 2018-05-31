/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"github.com/xelabs/go-mysqlstack/driver"

	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// handleSelect used to handle the select command.
func (spanner *Spanner) handleSelect(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	database := session.Schema()
	return spanner.Execute(session, database, query, node)
}

func (spanner *Spanner) handleSelectStream(session *driver.Session, query string, node sqlparser.Statement, callback func(qr *sqltypes.Result) error) error {
	streamBufferSize := 1024 * 1024 * 16 // 64MB
	database := session.Schema()
	return spanner.ExecuteStreamFetch(session, database, query, node, callback, streamBufferSize)
}

// handle select [dual]
func (spanner *Spanner) handleDual(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	return spanner.ExecuteSingle(query)
}
