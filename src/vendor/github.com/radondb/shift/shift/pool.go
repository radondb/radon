/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package shift

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/radondb/shift/xbase/sync2"
	"github.com/radondb/shift/xlog"
	"github.com/siddontang/go-mysql/client"
)

var (
	maxIdleTime = 20 // 20s
)

// Blocked connection pool.
type Pool struct {
	log   *xlog.Log
	conns chan *client.Conn
	mu    sync.Mutex

	host     string
	user     string
	password string

	// If maxIdleTime reached, the connection will be closed by get.
	maxIdleTime int64

	closed sync2.AtomicBool
	sem    *sync2.Semaphore
}

func NewPool(log *xlog.Log, cap int, host string, user string, password string) (*Pool, error) {
	conns := make(chan *client.Conn, cap)
	for i := 0; i < cap; i++ {
		to, err := client.Connect(host, user, password, "")
		if err != nil {
			log.Error("shift.new.pool.connection.error:%+v", err)
			return nil, err
		}
		to.SetTimestamp(time.Now().Unix())
		conns <- to
	}
	log.Info("shift.pool[host:%v, cap:%d].done", host, cap)

	return &Pool{
		log:         log,
		conns:       conns,
		host:        host,
		user:        user,
		password:    password,
		maxIdleTime: int64(maxIdleTime),
		closed:      sync2.NewAtomicBool(false),
		sem:         sync2.NewSemaphore(cap, 0),
	}, nil
}

func (p *Pool) Get() *client.Conn {
	log := p.log
	var conn *client.Conn

	// In case conns is empty, then we`ll meet blok if pool.get() acquire
	// the mu.lock() and will block as there`s no elements in conns. So before
	// get a conn from pool, we should make sure the pool is not empty.
	p.sem.Acquire()
	p.mu.Lock()
	if p.conns == nil {
		if p.isClosed() {
			log.Warning("shift.get.conn.but.pool.is.closed")
		}
		log.Error("shift.get.conn.but.conns.is.nil")
		p.mu.Unlock()
		return nil
	}

	conn = <-p.conns
	if conn == nil {
		log.Error("shift.get.conn.is.nil")
		p.mu.Unlock()
		return nil
	}
	p.mu.Unlock()

	// If the idle time more than 1s,
	// we will do a ping to check the connection is OK or NOT.
	now := time.Now().Unix()
	elapsed := (now - conn.Timestamp())
	if elapsed > 1 {
		// If elapsed time more than 20s, we create new one.
		if elapsed > atomic.LoadInt64(&p.maxIdleTime) {
			conn.Close()
			if c, err := p.reconnect(); err != nil {
				log.Error("shift.get.reconnect.error:%+v", err)
				return nil
			} else {
				return c
			}
		}

		if err := conn.Ping(); err != nil {
			log.Warning("shift.get.connection.was.bad, prepare.a.new.connection")
			if c, err := p.reconnect(); err != nil {
				log.Error("shift.get.reconnect.error:%+v", err)
				return nil
			} else {
				return c
			}
		}
	}
	return conn
}

func (p *Pool) Put(conn *client.Conn) {
	log := p.log
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.conns == nil {
		if p.isClosed() {
			log.Warning("shift.put.conn.but.pool.had.been.closed")
			conn.Close()
		} else {
			log.Error("shift.put.conn.but.conns.is.nil.and.pool.is.not.closed")
		}
		return
	}

	if conn == nil {
		log.Error("shift.put.conn.but.conn.is.nil")
		return
	}

	conn.SetTimestamp(time.Now().Unix())
	p.conns <- conn
	p.sem.Release()
}

func (p *Pool) Close() {
	log := p.log
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.isClosed() {
		log.Info("pool.had.been.closed.before.and.return.directly")
		return
	}

	close(p.conns)
	for conn := range p.conns {
		conn.Close()
	}
	p.conns = nil
	p.closed.Set(true)
}

func (p *Pool) isClosed() bool {
	return p.closed.Get()
}

func (p *Pool) reconnect() (*client.Conn, error) {
	log := p.log
	c, err := client.Connect(p.host, p.user, p.password, "")
	if err != nil {
		log.Error("shift.reconnect.new.conn.error:%+v", err)
		return nil, err
	}
	c.SetTimestamp(time.Now().Unix())
	return c, nil
}
