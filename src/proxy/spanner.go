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
	"config"
	"monitor"
	"plugins"
	"router"
	"sync"
	"xbase"
	"xbase/sync2"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// Spanner tuple.
type Spanner struct {
	log           *xlog.Log
	audit         *audit.Audit
	conf          *config.Config
	router        *router.Router
	scatter       *backend.Scatter
	sessions      *Sessions
	iptable       *IPTable
	throttle      *xbase.Throttle
	plugins       *plugins.Plugin
	diskChecker   *DiskCheck
	manager       *Manager
	readonly      sync2.AtomicBool
	mu            sync.RWMutex
	serverVersion string
}

// NewSpanner creates a new spanner.
func NewSpanner(log *xlog.Log, conf *config.Config,
	iptable *IPTable, router *router.Router, scatter *backend.Scatter, sessions *Sessions, audit *audit.Audit, throttle *xbase.Throttle, plugins *plugins.Plugin, serverVersion string) *Spanner {
	return &Spanner{
		log:           log,
		conf:          conf,
		audit:         audit,
		iptable:       iptable,
		router:        router,
		scatter:       scatter,
		sessions:      sessions,
		throttle:      throttle,
		plugins:       plugins,
		serverVersion: serverVersion,
	}
}

// Init used to init the async worker.
func (spanner *Spanner) Init() error {
	log := spanner.log
	conf := spanner.conf

	diskChecker := NewDiskCheck(log, conf.Audit.LogDir)
	if err := diskChecker.Init(); err != nil {
		return err
	}
	spanner.diskChecker = diskChecker

	mgr := NewManager(log, spanner.sessions, conf.Proxy)
	if err := mgr.Init(); err != nil {
		return err
	}
	spanner.manager = mgr
	return nil
}

// Close used to close spanner.
func (spanner *Spanner) Close() error {
	spanner.diskChecker.Close()
	spanner.manager.Close()
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

// SessionInc increase client connection metrics, it need the user is assigned
func (spanner *Spanner) SessionInc(s *driver.Session) {
	monitor.ClientConnectionInc(s.User())
}

// SessionDec decrease client connection metrics.
func (spanner *Spanner) SessionDec(s *driver.Session) {
	monitor.ClientConnectionDec(s.User())
}

// ServerVersion impl -- returns server version of Radon when greeting.
func (spanner *Spanner) ServerVersion() string {
	spanner.mu.RLock()
	defer spanner.mu.RUnlock()
	if spanner.serverVersion == "" {
		spanner.serverVersion = defaultMySQLVersionStr
	}
	return spanner.serverVersion
}

// SetServerVersion used to set serverVersion.
func (spanner *Spanner) SetServerVersion() {
	version, err := getBackendVersion(spanner)
	if err != nil {
		return
	}

	// get the org ServerVersion and tag
	orgServerVersion, err := parseVersionString(spanner.ServerVersion(), true)
	if version.equal(orgServerVersion) {
		return
	}
	version.Tag = orgServerVersion.Tag

	spanner.mu.Lock()
	defer spanner.mu.Unlock()
	spanner.serverVersion = version.toStr()
}

func (spanner *Spanner) isTwoPC() bool {
	return spanner.conf.Proxy.TwopcEnable
}

func (spanner *Spanner) isAutocommitFalseIsTxn() bool {
	return spanner.conf.Proxy.AutocommitFalseIsTxn
}

func (spanner *Spanner) isLowerCaseTableNames() bool {
	if spanner.conf.Proxy.LowerCaseTableNames == 0 {
		return false
	}
	return true
}
