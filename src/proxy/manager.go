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
	"sync"
	"time"

	"config"

	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

type Manager struct {
	log      *xlog.Log
	sessions *Sessions
	conf     *config.ProxyConfig
	done     chan bool
	ticker   *time.Ticker
	wg       sync.WaitGroup
	mu       sync.RWMutex
}

// https://www.percona.com/doc/percona-server/8.0/management/kill_idle_trx.html
func (mgr *Manager) killIdleTxn() error {
	log := mgr.log
	ss := mgr.sessions
	ssInTxn := ss.SnapshotTxn()

	for _, tsi := range ssInTxn {
		if mgr.conf.IdleTxnTimeout == 0 {
			break
		}

		if tsi.Time > mgr.conf.IdleTxnTimeout {
			if tsi.Info == sqlparser.BeginTxnStr || tsi.Info == "" {
				log.Warning("the session in transaction will be killed, the session info is: %v:", tsi)
				str := fmt.Sprintf("the session in transaction is idle for a long time: %d s > %d s.", tsi.Time, mgr.conf.IdleTxnTimeout)
				ss.Kill(tsi.ID, str)
			}
		}
	}
	return nil
}

func (mgr *Manager) manageMain() error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	if err := mgr.killIdleTxn(); err != nil {
		return err
	}
	return nil
}

func (mgr *Manager) manage() {
	defer mgr.ticker.Stop()
	for {
		select {
		case <-mgr.ticker.C:
			mgr.manageMain()
		case <-mgr.done:
			return
		}
	}
}

// Init used to init manager goroutine.
func (mgr *Manager) Init() error {
	log := mgr.log

	mgr.wg.Add(1)
	go func(mgr *Manager) {
		defer mgr.wg.Done()
		mgr.manage()
	}(mgr)

	log.Info("Manager.init.done")
	return nil
}

// Close used to close the goroutine.
func (mgr *Manager) Close() {
	close(mgr.done)
	mgr.wg.Wait()
}

// NewManager creates new Manager.
func NewManager(log *xlog.Log, sessions *Sessions, conf *config.ProxyConfig) *Manager {
	return &Manager{
		log:      log,
		sessions: sessions,
		conf:     conf,
		done:     make(chan bool),
		ticker:   time.NewTicker(time.Duration(time.Second)),
	}
}
