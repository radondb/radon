package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestSubTableToTable(t *testing.T) {
	testCases := []struct {
		in, out string
	}{
		{"t", ""},
		{"t1_0001", "t1"},
		{"t2_000", ""},
		{"t3_0000_0001", "t3_0000"},
	}

	for _, test := range testCases {
		isSub, table := SubTableToTable(test.in)
		if isSub || test.out != "" {
			assert.Equal(t, test.out, table)
		}
	}
}

func TestCtlV1ShardBalanceAdvice1(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	rdbs := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Databases",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("information_schema")),
			},
		},
	}

	r10 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "SizeInMB",
				Type: querypb.Type_DECIMAL,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("8192")),
			},
		},
	}

	r11 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "SizeInMB",
				Type: querypb.Type_DECIMAL,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("3072")),
			},
		},
	}

	r2 := &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "table_schema",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "table_name",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "sizeMB",
				Type: querypb.Type_DECIMAL,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1_0001")),
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("6144")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("t1_0002")),
				sqltypes.MakeTrusted(querypb.Type_DECIMAL, []byte("2048")),
			},
		},
	}

	// fakedbs.
	{
		fakedbs.AddQuery("show databases", rdbs)
		fakedbs.AddQuery("create database if not exists `test`", &sqltypes.Result{})
		fakedbs.AddQueryPattern("create .*", &sqltypes.Result{})
		fakedbs.AddQuerys("select round((sum(data_length) + sum(index_length)) / 1024/ 1024, 0)  as sizeinmb from information_schema.tables", r10, r11)
		fakedbs.AddQuery("SELECT table_schema, table_name, ROUND((SUM(data_length+index_length)) / 1024/ 1024, 0) AS sizeMB FROM information_schema.TABLES GROUP BY table_name HAVING SUM(data_length + index_length)>10485760 ORDER BY (data_length + index_length) DESC", r2)
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
		query := "create table test.t1(id int, b int) partition by hash(id)"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// radon rebalance.
	{
		query := "radon rebalance"
		_, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		scatter := proxy.Scatter()
		router := proxy.Router()
		spanner := proxy.Spanner()
		plugin := proxy.Plugins()

		rebalance := NewRebalance(log, scatter, router, spanner, proxy.Config(), plugin)
		_, err = rebalance.Rebalance()
		assert.NotNil(t, err)
	}
}

var (
	showBinlogFormat1 = &sqltypes.Result{
		RowsAffected: 1,
		Fields: []*querypb.Field{
			{
				Name: "Variable_name",
				Type: querypb.Type_VARCHAR,
			},
			{
				Name: "Value",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("binlog_format")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("ROW")),
			},
		},
	}
	selectResult = &sqltypes.Result{
		RowsAffected: 2,
		Fields: []*querypb.Field{
			{
				Name: "Tables_in_test",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("b")),
			},
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("A")),
			},
		},
	}

	emptyResult = &sqltypes.Result{}
)

func TestRebalanceMigrateErr(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, p, cleanup := MockProxy(log)
	defer cleanup()
	address := p.Address()

	// fakedbs.
	{
		fakedbs.AddQueryPattern("create .*", emptyResult)
		fakedbs.AddQueryPattern("show .*", showCreateTableResult)
		fakedbs.AddQuery("SHOW GLOBAL VARIABLES LIKE \"binlog_format\"", showBinlogFormat)
		fakedbs.AddQueryPattern("FLUSH .*", emptyResult)
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
		query := "create table test.a(i int primary key) single"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "radon rebalance"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	{
		fakedbs.AddQueryPattern("select .*", selectResult)
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "radon rebalance"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	{
		fakedbs.AddQueryPattern("select .*", selectResult)
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		query := "radon rebalance"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	scatter := p.Scatter()
	router := p.Router()
	spanner := p.Spanner()
	plugin := p.Plugins()

	max := &BackendSize{}
	min := &BackendSize{}
	{
		query := "radon rebalance"
		_, err := sqlparser.Parse(query)
		assert.Nil(t, err)

		rebalance := NewRebalance(log, scatter, router, spanner, p.Config(), plugin)
		err = RebalanceMigrate(log, rebalance, max, min, "", "")
		assert.NotNil(t, err)

	}

	// Set readonly, readonly forbid.
	{
		p.SetReadOnly(true)
		rebalance := NewRebalance(log, scatter, router, spanner, p.Config(), plugin)
		err := RebalanceMigrate(log, rebalance, max, min, "", "")
		assert.NotNil(t, err)
		p.SetReadOnly(false)
	}

	// backend is nil
	var from, fromUsr, fromPasswd, to, toUsr, toPasswd string
	backends := scatter.BackendConfigsClone()
	for _, backend := range backends {
		if backend.Name == "backend0" {
			from = backend.Address
			fromUsr = backend.User
			fromPasswd = backend.Password
		} else if backend.Name == "backend1" {
			to = backend.Address
			toUsr = backend.User
			toPasswd = backend.Password
		}
	}
	max = &BackendSize{
		Name:    "max",
		Address: from,
		Size:    0,
		User:    fromUsr,
		Passwd:  fromPasswd,
	}
	min = &BackendSize{
		Name:    "",
		Address: to,
		Size:    0,
		User:    toUsr,
		Passwd:  toPasswd,
	}
	// shift.WaitFinish() err.
	{
		fakedbs.AddQueryPattern("select .*", selectResult)
		rebalance := NewRebalance(log, scatter, router, spanner, p.Config(), plugin)
		err := RebalanceMigrate(log, rebalance, max, min, "test", "a")
		assert.NotNil(t, err)
	}

	// to backend is nil
	to = "192.168.0.1:3306"
	min = &BackendSize{
		Name:    "",
		Address: to,
		Size:    0,
		User:    toUsr,
		Passwd:  toPasswd,
	}
	{
		rebalance := NewRebalance(log, scatter, router, spanner, p.Config(), plugin)
		err := RebalanceMigrate(log, rebalance, max, min, "test", "a")
		assert.NotNil(t, err)
	}
}
