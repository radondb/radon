/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"time"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

type mode int

const (
	// R enum.
	R mode = iota
	// W enum.
	W
)

func (spanner *Spanner) auditLog(session *driver.Session, m mode, typ string, query string, qr *sqltypes.Result, failed bool) error {
	adit := spanner.audit
	user := session.User()
	host := session.Addr()
	connID := session.ID()
	affected := uint64(0)
	if qr != nil {
		affected = qr.RowsAffected
	}
	now := time.Now().UTC()
	status := uint32(0)
	if failed {
		status = uint32(1)
	}
	switch m {
	case R:
		adit.LogReadEvent(typ, user, host, connID, query, status, affected, now)
	case W:
		adit.LogWriteEvent(typ, user, host, connID, query, status, affected, now)
	}
	return nil
}
