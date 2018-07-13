/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"xbase/stats"
	"xbase/sync2"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var _ Connection = &connection{}

// Connection tuple.
type Connection interface {
	ID() uint32
	Dial() error
	Ping() error
	Close()
	Closed() bool
	LastErr() error
	UseDB(string) error
	Kill(string) error
	Recycle()
	Address() string
	SetTimestamp(int64)
	Timestamp() int64
	Execute(string) (*sqltypes.Result, error)
	ExecuteStreamFetch(string) (driver.Rows, error)
	ExecuteWithLimits(query string, timeout int, maxmem int) (*sqltypes.Result, error)
}

type connection struct {
	log          *xlog.Log
	connectionID uint32
	user         string
	password     string
	address      string
	charset      string

	pool *Pool

	// If lastErr is not nil, this connection should be closed.
	lastErr error

	killed sync2.AtomicBool
	driver driver.Conn

	// Recycle timestamp, in seconds.
	timestamp int64

	counters *stats.Counters
}

// NewConnection creates a new connection.
func NewConnection(log *xlog.Log, pool *Pool) Connection {
	conf := pool.conf
	return &connection{
		log:      log,
		pool:     pool,
		user:     conf.User,
		password: conf.Password,
		address:  conf.Address,
		charset:  conf.Charset,
		counters: pool.counters,
	}
}

// Dial used to create a new driver conn.
func (c *connection) Dial() error {
	var err error
	defer mysqlStats.Record("conn.dial", time.Now())

	if c.driver, err = driver.NewConn(c.user, c.password, c.address, "", c.charset); err != nil {
		c.log.Error("conn[%s].dial.error:%+v", c.address, err)
		c.counters.Add(poolCounterBackendDialError, 1)
		c.Close()
		return errors.New("Server maybe lost, please try again")
	}
	c.connectionID = c.driver.ConnectionID()
	return nil
}

// Ping used to do ping.
func (c *connection) Ping() error {
	return c.driver.Ping()
}

// ID returns the connection ID.
func (c *connection) ID() uint32 {
	return c.connectionID
}

// UseDB used to send a 'use database' query to MySQL.
// This is SQLCOM_CHANGE_DB command not COM_INIT_DB.
func (c *connection) UseDB(db string) error {
	if db != "" {
		query := fmt.Sprintf("use %s", db)
		if _, err := c.Execute(query); err != nil {
			return err
		}
	}
	return nil
}

// SetTimestamp used to set the timestamp.
func (c *connection) SetTimestamp(ts int64) {
	c.timestamp = ts
}

// Timestamp returns Timestamp of connection.
func (c *connection) Timestamp() int64 {
	return c.timestamp
}

// setDeadline used to set deadline for a query.
func (c *connection) setDeadline(timeout int) (chan bool, *sync.WaitGroup) {
	var wg sync.WaitGroup
	done := make(chan bool, 1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Millisecond)
	wg.Add(1)
	go func() {
		defer func() {
			wg.Done()
			cancel()
		}()
		select {
		case <-ctx.Done():
			c.counters.Add(poolCounterBackendExecuteTimeout, 1)
			c.killed.Set(true)
			reason := ctx.Err().Error()
			c.Kill(reason)
		case <-done:
			return
		}
	}()
	return done, &wg
}

// Execute used to execute a query through this connection without limits.
func (c *connection) Execute(query string) (*sqltypes.Result, error) {
	return c.ExecuteWithLimits(query, 0, 0)
}

// Execute used to execute a query through this connection.
// if timeout or memlimits is 0, means there is not limits.
func (c *connection) ExecuteWithLimits(query string, timeout int, memlimits int) (*sqltypes.Result, error) {
	var err error
	var qr *sqltypes.Result
	log := c.log
	defer mysqlStats.Record("Connection.Execute", time.Now())

	// Query details.
	qd := NewQueryDetail(c, query)
	qz.Add(qd)
	defer qz.Remove(qd)

	// timeout.
	if timeout > 0 {
		done, wg := c.setDeadline(timeout)
		if done != nil {
			defer func() {
				close(done)
				wg.Wait()
			}()
		}
	}

	// memory limits.
	checkFunc := func(rows driver.Rows) error {
		if memlimits > 0 {
			if rows.Bytes() > memlimits {
				c.counters.Add(poolCounterBackendExecuteMaxresult, 1)
				return fmt.Errorf("Query execution was interrupted, max memory usage[%d bytes] exceeded", memlimits)
			}
		}
		return nil
	}

	// execute.
	if qr, err = c.driver.FetchAllWithFunc(query, -1, checkFunc); err != nil {
		c.counters.Add(poolCounterBackendExecuteAllError, 1)
		log.Error("conn[%s].execute[%s].error:%+v", c.address, query, err)
		c.lastErr = err

		// Connection is killed.
		if c.killed.Get() {
			return nil, fmt.Errorf("Query execution was interrupted, timeout[%dms] exceeded", timeout)
		}

		// Connection is broken(closed by server).
		if err == io.EOF {
			return nil, errors.New("Server maybe lost, please try again")
		}
		return nil, err
	}
	return qr, nil
}

func (c *connection) ExecuteStreamFetch(query string) (driver.Rows, error) {
	return c.driver.Query(query)
}

// Kill used to kill current connection.
func (c *connection) Kill(reason string) error {
	c.counters.Add(poolCounterBackendKilled, 1)
	kill, err := c.pool.Get()
	if err != nil {
		return err
	}
	defer kill.Recycle()

	c.log.Warning("conn[%s, ID:%v].be.killed.by[%v].reason[%s]", c.address, c.ID(), kill.ID(), reason)
	query := fmt.Sprintf("KILL %d", c.connectionID)
	if _, err = kill.Execute(query); err != nil {
		c.log.Warning("conn[%s, ID:%v].kill.error:%+v", c.address, c.ID(), err)
		return err
	}
	return nil
}

// Recycle used to put current to pool.
func (c *connection) Recycle() {
	defer mysqlStats.Record("conn.recycle", time.Now())
	if !c.driver.Closed() {
		c.pool.Put(c)
	}
}

// Address returns the backend address of the connection.
func (c *connection) Address() string {
	return c.address
}

// Close used to close connection.
func (c *connection) Close() {
	defer mysqlStats.Record("conn.close", time.Now())
	c.lastErr = errors.New("I.am.closed")
	if c.driver != nil {
		c.driver.Close()
	}
}

func (c *connection) Closed() bool {
	if c.driver != nil {
		return c.driver.Closed()
	}
	return true
}

func (c *connection) LastErr() error {
	return c.lastErr
}
