/*
 * go-mydumper
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package common

import (
	"sync"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/xlog"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// Pool tuple.
type Pool struct {
	mu    sync.RWMutex
	log   *xlog.Log
	conns chan *Connection
}

// Connection tuple.
type Connection struct {
	ID     int
	client driver.Conn
}

// Execute used to executes the query.
func (conn *Connection) Execute(query string) error {
	return conn.client.Exec(query)
}

// Fetch used to fetch the results.
func (conn *Connection) Fetch(query string) (*sqltypes.Result, error) {
	return conn.client.FetchAll(query, -1)
}

// StreamFetch used to the results with streaming.
func (conn *Connection) StreamFetch(query string) (driver.Rows, error) {
	return conn.client.Query(query)
}

// NewPool creates the new pool.
func NewPool(log *xlog.Log, cap int, address string, user string, password string) (*Pool, error) {
	conns := make(chan *Connection, cap)
	for i := 0; i < cap; i++ {
		client, err := driver.NewConn(user, password, address, "", "utf8")
		if err != nil {
			return nil, err
		}
		conns <- &Connection{ID: i, client: client}
	}

	return &Pool{
		log:   log,
		conns: conns,
	}, nil
}

// Get used to get one connection from the pool.
func (p *Pool) Get() *Connection {
	conns := p.getConns()
	if conns == nil {
		return nil
	}
	conn := <-conns
	return conn
}

// Put used to put one connection to the pool.
func (p *Pool) Put(conn *Connection) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.conns == nil {
		return
	}
	p.conns <- conn
}

// Close used to close the pool and the connections.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	close(p.conns)
	for conn := range p.conns {
		conn.client.Close()
	}
	p.conns = nil
}

func (p *Pool) getConns() chan *Connection {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.conns
}
