/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"fmt"

	"github.com/xelabs/go-mysqlstack/driver"
)

// ComInitDB impl.
// Here, we will send a fake query 'SELECT 1' to the backend and check the 'USE DB'.
func (spanner *Spanner) ComInitDB(session *driver.Session, database string) error {
	router := spanner.router

	// Check the database ACL.
	if err := router.DatabaseACL(database); err != nil {
		return err
	}
	query := fmt.Sprintf("use %s", database)
	if _, err := spanner.ExecuteSingle(query); err != nil {
		return err
	}
	session.SetSchema(database)
	return nil
}
