/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"errors"
	"sync"
	"testing"
	"time"

	"xbase"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestRelay1(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxyWithBackup(log)
	defer cleanup()
	address := proxy.Address()
	backupRelay := proxy.spanner.BackupRelay()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create table `test`.`t1_0.*", &sqltypes.Result{})
		fakedbs.AddQuery("create table test.t1 (\n\t`id` int,\n\t`b` int\n) engine=tokudb", &sqltypes.Result{})
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}
	backupRelay.StopRelayWorker()
	backupRelay.StartRelayWorker()
	backupRelay.RelayRates()
	backupRelay.RelayStatus()
	backupRelay.RelayGTID()
	backupRelay.RelayCounts()
	backupRelay.RelayBinlog()
	backupRelay.RestartGTID()
}

func TestRelayDDLEngine(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	conf := MockDefaultConfig()
	conf.Proxy.BackupDefaultEngine = "innodb"
	fakedbs, proxy, cleanup := MockProxy2(log, conf)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create table `test`.`t1_0.*", &sqltypes.Result{})
		fakedbs.AddQuery("create table test.t1 (\n\t`id` int,\n\t`b` int\n) engine=innodb", &sqltypes.Result{})
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}
	time.Sleep(time.Second)
}

func TestRelayDDLAlter(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxyWithBackup(log)
	address := proxy.Address()
	defer cleanup()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create table `test`.`t1_0.*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("alter table `test`.`t1_0.*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("alter table test.t1 engine=tokudb", &sqltypes.Result{})
		fakedbs.AddQuery("create table test.t1 (\n\t`id` int,\n\t`b` int\n) engine=tokudb", &sqltypes.Result{})
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// alter test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "alter table test.t1 engine=tokudb"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}
	time.Sleep(time.Second)
}

func TestRelayDML1(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.ERROR))
	fakedbs, proxy, cleanup := MockProxyWithBackup(log)
	defer cleanup()
	address := proxy.Address()
	backupRelay := proxy.Spanner().BackupRelay()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create table `test`.`t.*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create table test.t.*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("insert into.*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("replace into.*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("delete from.*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("update.*", &sqltypes.Result{})
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// Set the max worker nums.
	backupRelay.SetMaxWorkers(3)
	n := 456
	dmls := []string{
		"insert into test.t1 (id, b) values(1,1)",
		"insert into test.t1 (id, b) values(1,1)",
		"insert into test.t1 (id, b) values(1,1)",
		"insert into test.t1 (id, b) values(1,1)",
		"insert into test.t1 (id, b) values(1,1)",
		"insert into test.t1 (id, b) values(1,1)",
		"insert into test.t1 (id, b) values(1,1)",
		"delete from test.t1 where id=1",
		"delete from test.t1 where id=1",
		"insert into test.t1 (id, b) values(1,1)",
		"delete from test.t1 where id=1",
		"delete from test.t1 where id=1",
		"update test.t1 set b=1 where id=1",
		"insert into test.t1 (id, b) values(1,1)",
		"update test.t1 set b=1 where id=1",
		"update test.t1 set b=1 where id=1",
		"update test.t1 set b=1 where id=1",
	}

	ddls := []string{
		"create table test.t2(id int, b int) partition by hash(id)",
		"create table test.t3(id int, b int) partition by hash(id)",
		"create table test.t4(id int, b int) partition by hash(id)",
		"create table test.t5(id int, b int) partition by hash(id)",
		"create table test.t6(id int, b int) partition by hash(id)",
	}
	want := (len(dmls)*n + (len(ddls) + 1))

	var wg sync.WaitGroup

	// dml routine.
	{
		wg.Add(1)
		go func() {
			defer wg.Done()

			client, err := driver.NewConn("mock", "mock", address, "", "utf8")
			assert.Nil(t, err)
			for i := 0; i < n; i++ {
				for _, dml := range dmls {
					if _, err = client.FetchAll(dml, -1); err != nil {
						log.Panic("---%v", err)
					}
				}
			}
		}()
	}

	// set parallel type.
	{
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				backupRelay.SetParallelType(int32(i % 5))
				time.Sleep(time.Millisecond * 33)
			}
		}()
	}

	// ddl routine.
	{
		wg.Add(1)
		go func() {
			defer wg.Done()

			client, err := driver.NewConn("mock", "mock", address, "", "utf8")
			assert.Nil(t, err)
			for _, ddl := range ddls {
				_, err = client.FetchAll(ddl, -1)
				assert.Nil(t, err)
				time.Sleep(time.Millisecond * 300)
			}
		}()
	}

	// check routine.
	{
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				counts := int(backupRelay.RelayCounts())
				if counts == want {
					break
				}
				time.Sleep(time.Millisecond * 100)
			}
		}()
	}

	backupRelay.ParallelWorkers()
	assert.Equal(t, 3, int(backupRelay.MaxWorkers()))

	wg.Wait()
	// 49 %5 = 4
	assert.Equal(t, 4, int(backupRelay.ParallelType()))
}

func TestRelayWaitForBackupWorkerDone(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxyWithBackup(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create table `test`.`t1_0.*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("insert into test.t1_0.*", &sqltypes.Result{})
		fakedbs.AddQueryDelay("insert into test.t1 (id, b) values(1,1)", &sqltypes.Result{}, 1000)
		fakedbs.AddQuery("create table test.t1 (\n\t`id` int,\n\t`b` int\n) engine=tokudb", &sqltypes.Result{})
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// dml.
	{
		querys := []string{
			"insert into test.t1 (id, b) values(1,1)",
		}
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		for i := 0; i < 100; i++ {
			for _, query := range querys {
				_, err = client.FetchAll(query, -1)
				assert.Nil(t, err)
			}
		}
	}
	time.Sleep(time.Second)
}

func TestRelayDMLErrorAndRelayAgain(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxyWithBackup(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create table `test`.`t1_0.*", &sqltypes.Result{})
		fakedbs.AddQuery("create table test.t1 (\n\t`id` int,\n\t`b` int\n) engine=tokudb", &sqltypes.Result{})
		fakedbs.AddQueryPattern("insert into test.t1_0.*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("delete from.*", &sqltypes.Result{})
		// Relay will be error.
		fakedbs.AddQueryErrorPattern("insert into test.t1 \\(id.*", errors.New("mock.relay.insert.error"))
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// dml.
	{
		querys := []string{
			"insert into test.t1 (id, b) values(1,1)",
			"delete from test.t1 where id=1",
		}
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		for _, query := range querys {
			_, err = client.FetchAll(query, -1)
			assert.Nil(t, err)
		}
		// Unsupport Event.
		proxy.spanner.binlog.LogEvent(xbase.SELECT, "test", "unsupport")
	}
	time.Sleep(time.Second)

	// Restart replay again.
	{
		proxy.spanner.backupRelay.StartRelayWorker()
	}
	time.Sleep(time.Second)
}

func TestRelayDDLError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxyWithBackup(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create table `test`.`t1_0.*", &sqltypes.Result{})
		// Relay will be error.
		fakedbs.AddQueryError("create table test.t1 (\n\t`id` int,\n\t`b` int\n) engine=tokudb", errors.New("mock.relay.create.table.error"))
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}
	time.Sleep(time.Second)
}

func TestRelayWithNoBackup(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	conf := MockDefaultConfig()
	conf.Binlog.EnableBinlog = true
	conf.Binlog.EnableRelay = true
	fakedbs, proxy, cleanup := MockProxy1(log, conf)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create table `test`.`t1_0.*", &sqltypes.Result{})
		fakedbs.AddQuery("create table test.t1 (\n\t`id` int,\n\t`b` int\n) engine=innodb", &sqltypes.Result{})
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}
	time.Sleep(time.Second)
}

func TestRelayRestartSQLWorker(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxyWithBackup(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create table `test`.`t1_0.*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("insert into test.t1_0.*", &sqltypes.Result{})
		fakedbs.AddQueryDelay("insert into test.t1 (id, b) values(1,1)", &sqltypes.Result{}, 1000)
		fakedbs.AddQuery("create table test.t1 (\n\t`id` int,\n\t`b` int\n) engine=tokudb", &sqltypes.Result{})
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// dml.
	{
		querys := []string{
			"insert into test.t1 (id, b) values(1,1)",
		}
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		for i := 0; i < 100; i++ {
			for _, query := range querys {
				_, err = client.FetchAll(query, -1)
				assert.Nil(t, err)
			}
		}
	}
	time.Sleep(time.Second)

	proxy.spanner.backupRelay.StopRelayWorker()
	proxy.spanner.backupRelay.ResetRelayWorker(time.Now().UTC().UnixNano())
	proxy.spanner.backupRelay.StartRelayWorker()
	time.Sleep(time.Second)
}
