package proxy

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// to test the coverage
func TestReshardShiftError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("insert .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("select .*", showTablesResult3)
		//fakedbs.AddQueryPattern("show .*", &sqltypes.Result{})
		fakedbs.AddQueryPattern("show .*", showCreateTableResult)
		fakedbs.AddQuery("SHOW GLOBAL VARIABLES LIKE \"binlog_format\"", showBinlogFormat)

	}

	// create database.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "create database test"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// create test table.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		querys := []string{
			"create table test.a(i int primary key) single",
		}
		for _, query := range querys {
			_, err = client.FetchAll(query, -1)
			assert.Nil(t, err)
		}
	}

	// Insert.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "insert into test.a (id, b) values(1),(3)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// radon reshard.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "radon reshard test.a to test.b"
		_, err = client.FetchAll(query, -1)

		time.Sleep(1 * time.Second)

		assert.Nil(t, err)
	}
}
