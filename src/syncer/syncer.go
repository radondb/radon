/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package syncer

import (
	"backend"
	"encoding/json"
	"os"
	"path"
	"sync"
	"time"

	"config"
	"router"
	"xbase"

	"github.com/xelabs/go-mysqlstack/xlog"
)

// Syncer tuple.
type Syncer struct {
	mu      sync.RWMutex
	wg      sync.WaitGroup
	log     *xlog.Log
	done    chan bool
	peer    *Peer
	metadir string
	ticker  *time.Ticker
	router  *router.Router
	scatter *backend.Scatter
}

// NewSyncer creates the new syncer.
func NewSyncer(log *xlog.Log, metadir string, peerAddr string, router *router.Router, scatter *backend.Scatter) *Syncer {
	return &Syncer{
		log:     log,
		metadir: metadir,
		router:  router,
		scatter: scatter,
		done:    make(chan bool),
		peer:    NewPeer(log, metadir, peerAddr),
		ticker:  time.NewTicker(time.Duration(time.Millisecond * 500)), // 0.5s
	}
}

// Init used to load the peers from the file and start the check thread.
func (s *Syncer) Init() error {
	log := s.log

	log.Info("syncer.init.metadir:%v", s.metadir)
	if err := os.MkdirAll(s.metadir, os.ModePerm); err != nil {
		return err
	}

	// Peers.
	if err := s.peer.LoadConfig(); err != nil {
		return err
	}
	log.Info("syncer.init.peers:%v", s.peer.peers)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.ticker.Stop()

		for {
			select {
			case <-s.ticker.C:
				s.check()
			case <-s.done:
				return
			}
		}
	}()
	log.Info("syncer.init.done")
	return nil
}

// Close used to close the syncer.
func (s *Syncer) Close() {
	close(s.done)
	s.wg.Wait()
}

// AddPeer used to add new peer to syncer.
func (s *Syncer) AddPeer(peer string) error {
	return s.peer.Add(peer)
}

// RemovePeer used to remove peer from syncer.
func (s *Syncer) RemovePeer(peer string) error {
	return s.peer.Remove(peer)
}

// Peers returns all the peers.
func (s *Syncer) Peers() []string {
	return s.peer.Clone()
}

// RLock used to acquire the lock of syncer.
func (s *Syncer) RLock() {
	s.mu.RLock()
}

// RUnlock used to release the lock of syncer.
func (s *Syncer) RUnlock() {
	s.mu.RUnlock()
}

func (s *Syncer) check() {
	log := s.log
	maxVer := int64(0)
	maxPeer := ""
	self := s.peer.self
	peers := s.peer.Clone()
	for _, peer := range peers {
		if peer != self {
			versionURL := "http://" + path.Join(peer, versionRestURL)
			peerVerStr, err := xbase.HTTPGet(versionURL)
			if err != nil {
				log.Error("syncer.check.version.get[%s].error:%+v", peerVerStr, err)
				continue
			}

			version := &config.Version{}
			if err := json.Unmarshal([]byte(peerVerStr), version); err != nil {
				log.Error("syncer.version.unmarshal[%s].error:%+v", peerVerStr, err)
				return
			}
			peerVer := version.Ts
			if peerVer > maxVer {
				maxVer = peerVer
				maxPeer = peer
			}
		}
	}

	selfVer := config.ReadVersion(s.metadir)
	if maxVer > selfVer {
		log.Warning("syncer.version[%v,%s].larger.than.self[%v, %s]", maxVer, maxPeer, selfVer, self)
		metaURL := "http://" + path.Join(maxPeer, metaRestURL)
		metaStr, err := xbase.HTTPGet(metaURL)
		if err != nil {
			log.Error("syncer.check.meta.get[%s].error:%+v", metaStr, err)
			return
		}

		meta := &Meta{}
		if err := json.Unmarshal([]byte(metaStr), meta); err != nil {
			log.Error("syncer.check.meta.unmarshal[%s].error:%+v", metaStr, err)
			return
		}
		s.MetaRebuild(meta)
		s.MetaReload()
	}
}
