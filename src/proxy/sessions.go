/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"sort"
	"sync"
	"time"

	"backend"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

type session struct {
	mu          sync.Mutex
	node        sqlparser.Statement
	query       string
	session     *driver.Session
	timestamp   int64
	transaction backend.Transaction
}

// Sessions tuple.
type Sessions struct {
	log *xlog.Log
	mu  sync.RWMutex
	// Key is session ID.
	sessions map[uint32]*session
}

// NewSessions creates new session.
func NewSessions(log *xlog.Log) *Sessions {
	return &Sessions{
		log:      log,
		sessions: make(map[uint32]*session),
	}
}

// Add used to add the session to map when session created.
func (ss *Sessions) Add(s *driver.Session) {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	ss.sessions[s.ID()] = &session{
		session:   s,
		timestamp: time.Now().Unix()}
}

func (ss *Sessions) txnAbort(txn backend.Transaction, node sqlparser.Statement) {
	log := ss.log
	// If transaction is not nil, means we can abort it when the session exit.
	// Here there is some races:
	// 1. if txn has finished, abort do nothing.
	// 2. if txn has aborted, finished do nothing.
	//
	// Txn abortable case:
	// 1. select query.
	// 2. DDL query.
	// If the client closed, txn will be abort by backend.
	if txn != nil && node != nil {
		switch node.(type) {
		case *sqlparser.Select, *sqlparser.DDL:
			if err := txn.Abort(); err != nil {
				log.Error("proxy.session.txn.abort.error:%+v", err)
				return
			}
		}
	}
}

// Remove used to remove the session from the map when session exit.
func (ss *Sessions) Remove(s *driver.Session) {
	ss.mu.Lock()
	session, ok := ss.sessions[s.ID()]
	if !ok {
		ss.mu.Unlock()
		return
	}
	session.mu.Lock()
	txn := session.transaction
	node := session.node
	session.mu.Unlock()
	delete(ss.sessions, s.ID())
	ss.mu.Unlock()

	// txn abort.
	ss.txnAbort(txn, node)
}

// Kill used to kill a live session.
// 1. remove from sessions list.
// 2. close the session from the server side.
// 3. abort the session's txn.
func (ss *Sessions) Kill(id uint32, reason string) {
	log := ss.log
	ss.mu.Lock()
	session, ok := ss.sessions[id]
	if !ok {
		ss.mu.Unlock()
		return
	}
	log.Warning("session.id[%v].killed.reason:%s", id, reason)
	session.mu.Lock()
	txn := session.transaction
	node := session.node
	sess := session.session
	session.mu.Unlock()

	delete(ss.sessions, id)
	ss.mu.Unlock()

	// 1.close the session connection from the server side.
	sess.Close()

	// 2. abort the txn.
	ss.txnAbort(txn, node)
}

// Reaches used to check whether the sessions count reaches(>=) the quota.
func (ss *Sessions) Reaches(quota int) bool {
	ss.mu.RLock()
	defer ss.mu.RUnlock()
	return (len(ss.sessions) >= quota)
}

// TxnBinding used to bind txn to the session.
func (ss *Sessions) TxnBinding(s *driver.Session, txn backend.Transaction, node sqlparser.Statement, query string) {
	ss.mu.RLock()
	session, ok := ss.sessions[s.ID()]
	if !ok {
		ss.mu.RUnlock()
		return
	}
	ss.mu.RUnlock()

	session.mu.Lock()
	defer session.mu.Unlock()
	q := query
	if len(query) > 128 {
		q = query[:128]
	}
	session.query = q
	session.node = node
	session.transaction = txn
	session.timestamp = time.Now().Unix()
}

// TxnUnBinding used to set transaction and node to nil.
func (ss *Sessions) TxnUnBinding(s *driver.Session) {
	ss.mu.RLock()
	session, ok := ss.sessions[s.ID()]
	if !ok {
		ss.mu.RUnlock()
		return
	}
	ss.mu.RUnlock()

	session.mu.Lock()
	defer session.mu.Unlock()
	session.node = nil
	session.query = ""
	session.transaction = nil
	session.timestamp = time.Now().Unix()
}

// Close used to close all sessions.
func (ss *Sessions) Close() {
	i := 0
	for {
		ss.mu.Lock()
		for k, v := range ss.sessions {
			v.mu.Lock()
			txn := v.transaction
			sess := v.session
			node := v.node
			v.mu.Unlock()
			if txn == nil {
				delete(ss.sessions, k)
				sess.Close()
			} else {
				// Try to abort READ-ONLY or DDL statement.
				ss.txnAbort(txn, node)
			}
		}
		c := len(ss.sessions)
		ss.mu.Unlock()

		if c > 0 {
			ss.log.Warning("session.wait.for.shutdown.live.txn:[%d].wait.seconds:%d", c, i)
			time.Sleep(time.Second)
			i++
		} else {
			break
		}
	}
}

// SessionInfo tuple.
type SessionInfo struct {
	ID           uint32
	User         string
	Host         string
	DB           string
	Command      string
	Time         uint32
	State        string
	Info         string
	RowsSent     uint64
	RowsExamined uint64
}

// Sort by id.
type sessionInfos []SessionInfo

// Len impl.
func (q sessionInfos) Len() int { return len(q) }

// Swap impl.
func (q sessionInfos) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

// Less impl.
func (q sessionInfos) Less(i, j int) bool { return q[i].ID < q[j].ID }

// Snapshot returns all session info.
func (ss *Sessions) Snapshot() []SessionInfo {
	var infos sessionInfos

	now := time.Now().Unix()
	ss.mu.Lock()
	for _, v := range ss.sessions {
		v.mu.Lock()
		defer v.mu.Unlock()
		info := SessionInfo{
			ID:      v.session.ID(),
			User:    v.session.User(),
			Host:    v.session.Addr(),
			DB:      v.session.Schema(),
			Command: "Sleep",
			Time:    uint32(now - v.timestamp),
		}

		if v.node != nil {
			info.Command = "Query"
			info.Info = v.query
		}
		infos = append(infos, info)
	}
	ss.mu.Unlock()
	sort.Sort(infos)
	return infos
}
