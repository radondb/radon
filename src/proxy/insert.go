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

// handleInsert used to handle the insert command.
func (spanner *Spanner) handleInsert(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	database := session.Schema()
	return spanner.Execute(session, database, query, node)
}
