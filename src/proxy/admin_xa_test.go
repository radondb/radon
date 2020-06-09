package proxy

import (
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqldb"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
	"github.com/xelabs/go-mysqlstack/xlog"
	"testing"
)

var (
	resultXARecover = &sqltypes.Result{
		RowsAffected: 2,
		Fields: []*querypb.Field{
			{
				Name: "formatID",
				Type: querypb.Type_INT32,
			},
			{
				Name: "gtrid_length",
				Type: querypb.Type_INT32,
			},
			{
				Name: "bqual_length",
				Type: querypb.Type_INT32,
			},
			{
				Name: "data",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("1")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("22")),
				sqltypes.MakeTrusted(querypb.Type_INT32, []byte("0")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("RXID-20200429140200-39")),
			},
		},
	}
)

func TestAdminXaRecover(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	// fakedbs.
	fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
	fakedbs.AddQuery("xa recover", resultXARecover)

	// radon xa recover.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "radon xa recover"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}
}

func TestAdminXaRecoverFailed(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
	fakedbs.AddQueryError("xa recover", sqldb.NewSQLError(sqldb.ER_UNKNOWN_ERROR, "query.error"))

	client, err := driver.NewConn("mock", "mock", address, "", "utf8")
	assert.Nil(t, err)
	// radon xa recover failed.
	{
		query := "radon xa recover"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	{

		query := "radon xa commit"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	{
		query := "radon xa rollback"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

	client.Close()
}

func TestAdminXaCommit(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
	fakedbs.AddQuery("xa recover", resultXARecover)
	fakedbs.AddQueryPattern("xa commit .*", &sqltypes.Result{})

	// radon xa commit.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "radon xa commit"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}
}

func TestAdminXaCommitFailed(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
	fakedbs.AddQuery("xa recover", resultXARecover)
	fakedbs.AddQueryErrorPattern("XA ROLLBACK .*", sqldb.NewSQLError(sqldb.ER_UNKNOWN_ERROR, "query.error"))

	// radon xa commit.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "radon xa commit"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}

}

func TestAdminXaRollback(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
	fakedbs.AddQuery("xa recover", resultXARecover)
	fakedbs.AddQueryPattern("xa rollback .*", &sqltypes.Result{})

	// radon xa rollback failed.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "radon xa rollback"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}
}

func TestAdminXaRollbackFailed(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
	fakedbs.AddQuery("xa recover", resultXARecover)
	fakedbs.AddQueryErrorPattern("xa rollback .*", sqldb.NewSQLError(sqldb.ER_UNKNOWN_ERROR, "query.error"))

	// radon xa rollback.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "radon xa rollback"
		_, err = client.FetchAll(query, -1)
		assert.NotNil(t, err)
	}
}

func TestAdminXaCommitEmpty(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	fakedbs, proxy, cleanup := MockProxy(log)
	defer cleanup()
	address := proxy.Address()

	fakedbs.AddQueryPattern("use .*", &sqltypes.Result{})
	fakedbs.AddQuery("xa recover", &sqltypes.Result{})
	fakedbs.AddQueryErrorPattern("xa rollback .*", sqldb.NewSQLError(sqldb.ER_UNKNOWN_ERROR, "query.error"))

	// radon xa commit.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "radon xa commit"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}

	// radon xa rollback.
	{
		client, err := driver.NewConn("mock", "mock", address, "", "utf8")
		assert.Nil(t, err)
		defer client.Close()
		query := "radon xa rollback"
		_, err = client.FetchAll(query, -1)
		assert.Nil(t, err)
	}
}
