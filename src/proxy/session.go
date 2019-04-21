/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"sync"
	"time"

	"backend"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

type bitmask uint32

// session variables capabilities.
const (
	cap_streaming_fetch bitmask = 1 << iota // streaming fetch for this session
)

type session struct {
	log          *xlog.Log
	mu           sync.Mutex // Race with snapshot
	node         sqlparser.Statement
	query        string
	session      *driver.Session
	timestamp    int64
	capabilities bitmask
	transaction  backend.Transaction
}

func (s *session) setStreamingFetchVar(r bool) {
	if r {
		s.capabilities |= cap_streaming_fetch
	} else {
		s.capabilities &= ^cap_streaming_fetch
	}
}

func (s *session) getStreamingFetchVar() bool {
	return s.capabilities&cap_streaming_fetch != 0
}

func newSession(log *xlog.Log, s *driver.Session) *session {
	log.Debug("session[%v].created", s.ID())
	return &session{
		log:       log,
		session:   s,
		timestamp: time.Now().Unix(),
	}
}

func (s *session) close() {
	log := s.log
	id := s.session.ID()

	// close the session connection from the server side.
	s.session.Close()

	s.mu.Lock()
	node := s.node
	transaction := s.transaction
	s.mu.Unlock()
	log.Debug("session[%v].close.txn:%+v.node:%+v", id, transaction, node)

	// If transaction is not nil, means we can abort it when the session exit.
	// Here there is some races:
	// 1. if txn has finished, abort do nothing.
	// 2. if txn has aborted, finished do nothing.
	//
	// Txn abortable case:
	// 1. select query.
	// 2. DDL query.
	// If the client closed, txn will be abort by backend.
	if transaction != nil && node != nil {
		switch node.(type) {
		case *sqlparser.Select, *sqlparser.DDL:
			if err := transaction.Abort(); err != nil {
				log.Error("session.close.txn.abort.error:%+v", err)
				return
			}
		case *sqlparser.Transaction:
			if err := transaction.Abort(); err != nil {
				log.Error("session.close.txn.abort.error:%+v", err)
				return
			}
		}
	}
}
