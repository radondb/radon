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

// handleUseDB used to handle the UseDB command.
// Here, we will send a fake query 'SELECT 1' to the backend and check the 'USE DB'.
func (spanner *Spanner) handleUseDB(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	usedb := node.(*sqlparser.Use)
	db := usedb.DBName.String()
	router := spanner.router
	// Check the database ACL.
	if err := router.DatabaseACL(db); err != nil {
		return nil, err
	}

	if _, err := spanner.ExecuteSingle(query); err != nil {
		return nil, err
	}
	session.SetSchema(db)
	return &sqltypes.Result{}, nil
}
