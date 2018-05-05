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

func (spanner *Spanner) auditLog(session *driver.Session, m mode, typ string, query string, qr *sqltypes.Result) error {
	adit := spanner.audit
	user := session.User()
	host := session.Addr()
	connID := session.ID()
	affected := uint64(0)
	if qr != nil {
		affected = uint64(len(qr.Rows))
	}
	now := time.Now().UTC()
	switch m {
	case R:
		adit.LogReadEvent(typ, user, host, connID, query, affected, now)
	case W:
		adit.LogWriteEvent(typ, user, host, connID, query, affected, now)
	}
	return nil
}
