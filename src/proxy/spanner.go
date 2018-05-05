/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"audit"
	"backend"
	"binlog"
	"config"
	"router"
	"xbase"
	"xbase/sync2"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// Spanner tuple.
type Spanner struct {
	log         *xlog.Log
	audit       *audit.Audit
	conf        *config.Config
	router      *router.Router
	scatter     *backend.Scatter
	binlog      *binlog.Binlog
	sessions    *Sessions
	iptable     *IPTable
	throttle    *xbase.Throttle
	backupRelay *BackupRelay
	diskChecker *DiskCheck
	readonly    sync2.AtomicBool
}

// NewSpanner creates a new spanner.
func NewSpanner(log *xlog.Log, conf *config.Config,
	iptable *IPTable, router *router.Router, scatter *backend.Scatter, binlog *binlog.Binlog, sessions *Sessions, audit *audit.Audit, throttle *xbase.Throttle) *Spanner {
	return &Spanner{
		log:      log,
		conf:     conf,
		audit:    audit,
		iptable:  iptable,
		router:   router,
		scatter:  scatter,
		binlog:   binlog,
		sessions: sessions,
		throttle: throttle,
	}
}

// Init used to init the async worker.
func (spanner *Spanner) Init() error {
	log := spanner.log
	conf := spanner.conf

	backupRelay := NewBackupRelay(log, conf.Binlog, spanner)
	if err := backupRelay.Init(); err != nil {
		return err
	}
	spanner.backupRelay = backupRelay

	diskChecker := NewDiskCheck(log, conf.Binlog.LogDir)
	if err := diskChecker.Init(); err != nil {
		return err
	}
	spanner.diskChecker = diskChecker
	return nil
}

// Close used to close spanner.
func (spanner *Spanner) Close() error {
	spanner.backupRelay.Close()
	spanner.diskChecker.Close()
	spanner.log.Info("spanner.closed...")
	return nil
}

// ReadOnly returns the readonly or not.
func (spanner *Spanner) ReadOnly() bool {
	return spanner.readonly.Get()
}

// SetReadOnly used to set readonly.
func (spanner *Spanner) SetReadOnly(val bool) {
	spanner.readonly.Set(val)
}

// NewSession impl.
func (spanner *Spanner) NewSession(s *driver.Session) {
	spanner.sessions.Add(s)
}

// SessionClosed impl.
func (spanner *Spanner) SessionClosed(s *driver.Session) {
	spanner.sessions.Remove(s)
}

// BackupRelay returns BackupRelay tuple.
func (spanner *Spanner) BackupRelay() *BackupRelay {
	return spanner.backupRelay
}

func (spanner *Spanner) isTwoPC() bool {
	return spanner.conf.Proxy.TwopcEnable
}
