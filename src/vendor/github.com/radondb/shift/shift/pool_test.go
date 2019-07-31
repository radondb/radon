/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package shift

import (
	"fmt"
	"testing"
	"time"

	"github.com/radondb/shift/xbase/sync2"
	"github.com/radondb/shift/xlog"

	"github.com/siddontang/go-mysql/client"
	"github.com/stretchr/testify/assert"
)

func TestPool(t *testing.T) {
	// Config for normal
	cfg := &Config{
		From:         "127.0.0.1:3306",
		FromUser:     "root",
		FromDatabase: "shift_test_from",
		FromTable:    "t1",

		To:         "127.0.0.1:3307",
		ToUser:     "root",
		ToDatabase: "shift_test_to",
		ToTable:    "t1",

		Cleanup:   true,
		Threads:   16,
		Behinds:   256,
		MySQLDump: "mysqldump",
		RadonURL:  fmt.Sprintf("http://127.0.0.1:%d", restfulPort),
		Checksum:  true,
	}

	log := xlog.NewStdLog(xlog.Level(xlog.DEBUG))
	poolNormal, err := NewPool(log, 4, cfg.From, cfg.FromUser, cfg.FromPassword)
	assert.Nil(t, err)

	var conn *client.Conn
	// Test normal Get(), conn is not nil
	conn = poolNormal.Get()
	assert.NotNil(t, conn)

	// Test pool close first, conn we get is nil
	poolNormal.Close()
	conn = poolNormal.Get()
	assert.Nil(t, conn)

	// Test exception case, when we get conn from pool in one goroutine, and
	// pool is closed in another goroutine, the conns may be closed and will be set nil, e.g.
	// thread1: Get()-->getConns()-->we get a conns pointer and conns is not nil-->thread 2: Closed()-->
	// closed conns and the elements in conns-->get conn from conns(<-conns)-->then we got nil....
	poolError, err1 := fakeNewPool(log, 4, cfg.From, cfg.FromUser, cfg.FromPassword)
	assert.Nil(t, err1)
	// Here conns is not nil althouth it has 4 nil elements in chan.
	conn = poolError.Get()
	assert.Nil(t, conn)
}

func fakeNewPool(log *xlog.Log, cap int, host string, user string, password string) (*Pool, error) {
	conns := make(chan *client.Conn, cap)
	for i := 0; i < cap; i++ {
		conns <- nil
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

func TestConnReconnect(t *testing.T) {
	// Config for normal
	cfg := &Config{
		From:         "127.0.0.1:3306",
		FromUser:     "root",
		FromDatabase: "shift_test_from",
		FromTable:    "t1",

		To:         "127.0.0.1:3307",
		ToUser:     "root",
		ToDatabase: "shift_test_to",
		ToTable:    "t1",

		Cleanup:   true,
		Threads:   16,
		Behinds:   256,
		MySQLDump: "mysqldump",
		RadonURL:  fmt.Sprintf("http://127.0.0.1:%d", restfulPort),
		Checksum:  true,
	}

	maxIdleTime = 1
	log := xlog.NewStdLog(xlog.Level(xlog.DEBUG))
	pool, err := NewPool(log, 4, cfg.From, cfg.FromUser, cfg.FromPassword)
	// Sleep 1.1s to make sure elapsed time > maxIdleTime and reconnect
	time.Sleep(1100 * time.Millisecond)
	assert.Nil(t, err)

	var conn *client.Conn
	// Test normal Get(), conn is not nil
	conn = pool.Get()
	assert.NotNil(t, conn)
}

func TestGetConnBlockOrDeadlock(t *testing.T) {
	// Config for normal
	cfg := &Config{
		From:         "127.0.0.1:3306",
		FromUser:     "root",
		FromDatabase: "shift_test_from",
		FromTable:    "t1",

		To:         "127.0.0.1:3307",
		ToUser:     "root",
		ToDatabase: "shift_test_to",
		ToTable:    "t1",

		Cleanup:   true,
		Threads:   16,
		Behinds:   256,
		MySQLDump: "mysqldump",
		RadonURL:  fmt.Sprintf("http://127.0.0.1:%d", restfulPort),
		Checksum:  true,
	}

	log := xlog.NewStdLog(xlog.Level(xlog.DEBUG))
	pool, err := NewPool(log, 4, cfg.From, cfg.FromUser, cfg.FromPassword)
	assert.Nil(t, err)
	// First, we make the pool empty and leave one element
	c1 := pool.Get()
	go func() {
		c2 := pool.Get()
		c3 := pool.Get()
		c4 := pool.Get()
		assert.Equal(t, 0, len(pool.conns))
		log.Info("here.conns.len.should.be.0.and.real.len.is:%+v", len(pool.conns))

		c5 := pool.Get() // block 1s
		defer func() {
			pool.Put(c2)
			pool.Put(c3)
			pool.Put(c4)
			pool.Put(c5)
		}()
	}()
	time.Sleep(1 * time.Second)
	log.Info("sleep 1s")
	pool.Put(c1)
}
