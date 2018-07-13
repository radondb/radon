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

// handleKill used to handle the KILL command.
func (spanner *Spanner) handleKill(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	log := spanner.log
	kill := node.(*sqlparser.Kill)
	id := uint32(kill.QueryID.AsUint64())
	log.Warning("proxy.handleKill[%d].from.session[%v]", id, session.ID())
	sessions := spanner.sessions
	sessions.Kill(id, "kill.query.from.client")
	return &sqltypes.Result{}, nil
}
