/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package privilege

import (
	"github.com/xelabs/go-mysqlstack/sqlparser"
)

type PrivilegeHandler interface {
	Init() error
	Check(db string, user string, node sqlparser.Statement) error
	CheckPrivilege(db string, user string, node sqlparser.Statement) bool
	CheckUserPrivilegeIsSet(user string) bool
	IsSuperPriv(user string) bool
	GetUserPrivilegeDBS(user string) (dbs map[string]struct{})
	CheckDBinUserPrivilege(user string, db string) bool
	Close() error
}
