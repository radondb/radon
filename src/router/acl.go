/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package router

import (
	"strings"
)

var (
	systemDatabases = []string{"SYS", "MYSQL", "INFORMATION_SCHEMA", "PERFORMANCE_SCHEMA"}
)

// DatabaseACL tuple.
type DatabaseACL struct {
	acls map[string]string
}

// NewDatabaseACL creates new database acl.
func NewDatabaseACL() *DatabaseACL {
	acls := make(map[string]string)
	for _, db := range systemDatabases {
		acls[db] = db
	}
	return &DatabaseACL{acls}
}

// Allow used to check to see if the db is system database.
func (acl *DatabaseACL) Allow(db string) bool {
	db = strings.ToUpper(db)
	if _, ok := acl.acls[db]; !ok {
		return true
	}
	return false
}
