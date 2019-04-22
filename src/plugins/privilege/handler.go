/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package privilege

import (
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
)

type PrivilegeHandler interface {
	Init() error
	Check(session *driver.Session, node sqlparser.Statement) error
	CheckPrivilege(db string, user string, node sqlparser.Statement) bool
	Close() error
}
