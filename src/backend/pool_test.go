/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package backend

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestPoolz(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	// MySQL Server starts 1...
	th1 := driver.NewTestHandler(log)
	svr1, err := driver.MockMysqlServer(log, th1)
	assert.Nil(t, err)
	defer svr1.Close()
	addr1 := svr1.Addr()

	// MySQL Server starts 2...
	th2 := driver.NewTestHandler(log)
	svr2, err := driver.MockMysqlServer(log, th2)
	assert.Nil(t, err)
	defer svr2.Close()
	addr2 := svr2.Addr()

	// Connection
	conf := MockBackendConfigReplica("node1", addr1, addr2)
	conf.MaxConnections = 64
	poolz := NewPoolz(log, conf)
	defer poolz.Close()

	// json.
	{
		want := "{'name': 'node1@" + addr1 + "', 'capacity': 64, 'counters': {}}, {'name': 'node1@" + addr2 + "', 'capacity': 64, 'counters': {}}"
		got := poolz.JSON()
		assert.Equal(t, want, got)
	}
}

func TestPool(t *testing.T) {
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	// MySQL Server starts...
	th := driver.NewTestHandler(log)
	svr, err := driver.MockMysqlServer(log, th)
	assert.Nil(t, err)
	defer svr.Close()
	addr := svr.Addr()

	// Connection
	conf := MockBackendConfigDefault("node1", addr)
	conf.MaxConnections = 64
	pool := NewPool(log, conf, addr)

	// get
	{
		_, err := pool.Get()
		assert.Nil(t, err)
	}

	// put
	{
		for i := 0; i < conf.MaxConnections+100; i++ {
			conn := NewConnection(log, pool)
			err = conn.Dial()
			assert.Nil(t, err)
			pool.Put(conn)
		}
		want := "{'name': 'node1@" + pool.address + "', 'capacity': 64, 'counters': {'#pool.get': 1, '#pool.miss': 1, '#pool.put': 164}}"
		got := pool.JSON()
		assert.Equal(t, want, got)
	}

	// clean
	{
		pool.Close()
		_, err = pool.Get()
		assert.NotNil(t, err)
	}
}

func TestPoolConcurrent(t *testing.T) {
	var wg sync.WaitGroup
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	// MySQL Server starts...
	th := driver.NewTestHandler(log)
	svr, err := driver.MockMysqlServer(log, th)
	assert.Nil(t, err)
	defer svr.Close()
	addr := svr.Addr()

	// Connection
	conf := MockBackendConfigDefault(addr, addr)
	conf.MaxConnections = 64
	pool := NewPool(log, conf, addr)

	ch1 := make(chan bool)
	ch2 := make(chan bool)
	// get
	{
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ch1:
					return
				default:
					pool.Get()
				}
			}
		}()
	}

	// put
	{
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ch2:
					return
				default:
					conn := NewConnection(log, pool)
					conn.Dial()
					pool.Put(conn)
				}
			}
		}()
	}

	time.Sleep(time.Second)
	pool.Close()

	close(ch1)
	close(ch2)
	wg.Wait()
}

func TestPoolTimeout(t *testing.T) {
	var wg sync.WaitGroup
	defer leaktest.Check(t)()
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	// MySQL Server starts...
	th := driver.NewTestHandler(log)
	svr, err := driver.MockMysqlServer(log, th)
	assert.Nil(t, err)
	defer svr.Close()
	addr := svr.Addr()

	// Connection
	conf := MockBackendConfigDefault(addr, addr)
	conf.MaxConnections = 64
	pool := NewPool(log, conf, addr)

	ch2 := make(chan bool)

	// put
	{
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ch2:
					return
				default:
					conn := NewConnection(log, pool)
					conn.Dial()
					pool.Put(conn)
				}
			}
		}()
	}

	time.Sleep(time.Second * 2)
	// Reset maxIdleTime
	atomic.StoreInt64(&pool.maxIdleTime, 1)
	for i := 0; i < 100; i++ {
		pool.Get()
	}

	// Reset maxIdleTime
	atomic.StoreInt64(&pool.maxIdleTime, 10)
	time.Sleep(time.Second * 2)
	for i := 0; i < 100; i++ {
		pool.Get()
	}
	pool.Close()
	close(ch2)
	wg.Wait()
}
