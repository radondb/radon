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
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// handleKill used to handle the KILL command.
//mysql> show processlist;
//+----+------+-----------------+------+---------+------+----------+------------------+-----------+---------------+
//| Id | User | Host            | db   | Command | Time | State    | Info             | Rows_sent | Rows_examined |
//+----+------+-----------------+------+---------+------+----------+------------------+-----------+---------------+
//| 63 | root | 127.0.0.1:60216 | NULL | Sleep   |    4 |          | NULL             |         0 |             0 |
//| 67 | root | 127.0.0.1:60762 | NULL | Query   |    0 | starting | show processlist |         0 |             0 |
//| 68 | a    | 127.0.0.1:60765 | NULL | Sleep   |   12 |          | NULL             |         0 |             0 |
//+----+------+-----------------+------+---------+------+----------+------------------+-----------+---------------+

//mysql> show processlist;
//+----+------+-----------------+------+---------+------+----------+------------------+-----------+---------------+
//| Id | User | Host            | db   | Command | Time | State    | Info             | Rows_sent | Rows_examined |
//+----+------+-----------------+------+---------+------+----------+------------------+-----------+---------------+
//| 68 | a    | 127.0.0.1:60765 | NULL | Query   |    0 | starting | show processlist |         0 |             0 |
//+----+------+-----------------+------+---------+------+----------+------------------+-----------+---------------+
//mysql> kill 67;
//ERROR 1095 (HY000): You are not owner of thread 67
//mysql> kill 66;
//ERROR 1094 (HY000): Unknown thread id: 66
func (spanner *Spanner) handleKill(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	log := spanner.log
	kill := node.(*sqlparser.Kill)
	id := uint32(kill.QueryID.AsUint64())
	log.Warning("proxy.handleKill[%d].from.session[%v]", id, session.ID())
	sessions := spanner.sessions

	privilegePlug := spanner.plugins.PlugPrivilege()
	if !privilegePlug.IsSuperPriv(session.User()) {
		needKill := sessions.getSession(id)
		if needKill.session.User() != session.User() {
			return nil, sqldb.NewSQLErrorf(sqldb.ER_KILL_DENIED_ERROR, "You are not owner of thread %d", id)
		}
	}

	sessions.Kill(id, "kill.query.from.client")
	return &sqltypes.Result{}, nil
}
