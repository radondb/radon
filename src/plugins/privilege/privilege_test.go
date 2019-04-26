/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package privilege

import (
	"testing"

	"proxy"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"

	"github.com/xelabs/go-mysqlstack/sqlparser"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

var (
	userRs = &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Host",
				Type: querypb.Type_VARCHAR,
			},

			{
				Name: "User",
				Type: querypb.Type_VARCHAR,
			},

			{
				Name: "Select_priv",
				Type: querypb.Type_ENUM,
			},

			{
				Name: "Insert_priv",
				Type: querypb.Type_ENUM,
			},

			{
				Name: "Update_priv",
				Type: querypb.Type_ENUM,
			},

			{
				Name: "Delete_priv",
				Type: querypb.Type_ENUM,
			},

			{
				Name: "Create_priv",
				Type: querypb.Type_ENUM,
			},

			{
				Name: "Drop_priv",
				Type: querypb.Type_ENUM,
			},

			{
				Name: "Alter_priv",
				Type: querypb.Type_ENUM,
			},

			{
				Name: "Index_priv",
				Type: querypb.Type_ENUM,
			},

			{
				Name: "Show_db_priv",
				Type: querypb.Type_ENUM,
			},

			{
				Name: "Super_priv",
				Type: querypb.Type_ENUM,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("%")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("mock")),
			},
		},
	}

	dbRs = &sqltypes.Result{
		Fields: []*querypb.Field{
			{
				Name: "Host",
				Type: querypb.Type_VARCHAR,
			},

			{
				Name: "User",
				Type: querypb.Type_VARCHAR,
			},

			{
				Name: "Select_priv",
				Type: querypb.Type_ENUM,
			},

			{
				Name: "Insert_priv",
				Type: querypb.Type_ENUM,
			},

			{
				Name: "Update_priv",
				Type: querypb.Type_ENUM,
			},

			{
				Name: "Delete_priv",
				Type: querypb.Type_ENUM,
			},

			{
				Name: "Create_priv",
				Type: querypb.Type_ENUM,
			},

			{
				Name: "Drop_priv",
				Type: querypb.Type_ENUM,
			},

			{
				Name: "Alter_priv",
				Type: querypb.Type_ENUM,
			},

			{
				Name: "Index_priv",
				Type: querypb.Type_ENUM,
			},

			{
				Name: "Db",
				Type: querypb.Type_VARCHAR,
			},
		},
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("%")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("mock")),
			},
		},
	}
)

func TestLoadUserPrivilege(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.ERROR))

	// Init privileges.
	{
		// userRS.
		for i := 2; i < len(userRs.Fields); i++ {
			userRs.Rows[0] = append(userRs.Rows[0], sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")))
		}

		// dbRs.
		for i := 2; i < len(userRs.Fields); i++ {
			dbRs.Rows[0] = append(dbRs.Rows[0], sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")))
		}
		dbRs.Rows[0] = append(dbRs.Rows[0], sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test")))
	}

	// Create scatter and query handler.
	fakedbs, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()
	scatter := proxy.Scatter()

	fakedbs.AddQuery("select host, user, select_priv, insert_priv, update_priv, delete_priv, create_priv, drop_priv, alter_priv, index_priv, show_db_priv, super_priv from mysql.user", userRs)
	fakedbs.AddQueryPattern("select host, user, select_priv, insert_priv, update_priv, delete_priv, create_priv, drop_priv, grant_priv, alter_priv, index_priv, db from .*", dbRs)

	handler := NewPrivilege(log, nil, scatter)
	err := handler.Init()
	assert.Nil(t, err)
	defer handler.Close()

	tests := []struct {
		name string
		db   string
		user string
		sql  string
		err  string
	}{
		{
			name: "select.ok",
			db:   "test",
			user: "mock",
			sql:  "select * from t1",
			err:  "",
		},

		{
			name: "select.ok",
			db:   "test",
			user: "mock",
			sql:  "select * from test.t1",
			err:  "",
		},

		{
			name: "insert.ok",
			db:   "test",
			user: "mock",
			sql:  "insert into t1(a) values(1)",
			err:  "",
		},

		{
			name: "update.ok",
			db:   "test",
			user: "mock",
			sql:  "update t1 set a=a+1 where a=1",
			err:  "",
		},

		{
			name: "delete.ok",
			db:   "test",
			user: "mock",
			sql:  "delete from t1",
			err:  "",
		},

		{
			name: "show.ok",
			db:   "test",
			user: "mock",
			sql:  "show tables",
			err:  "",
		},

		{
			name: "ddl.ok",
			db:   "test",
			user: "mock",
			sql:  "create table t1(a int)",
			err:  "",
		},

		{
			name: "ddl.ok",
			db:   "test",
			user: "mock",
			sql:  "create table t1(a int)",
			err:  "",
		},

		{
			name: "node.nil.ok",
			db:   "test",
			user: "mock",
			sql:  "",
			err:  "",
		},

		{
			name: "user.not.exists",
			db:   "test",
			user: "mock1",
			sql:  "",
			err:  "Access denied for user 'mock1'@'test' (errno 1045) (sqlstate 28000)",
		},

		{
			name: "user.not.exists",
			db:   "test",
			user: "mock1",
			sql:  "",
			err:  "Access denied for user 'mock1'@'test' (errno 1045) (sqlstate 28000)",
		},
	}

	for _, test := range tests {
		var err error
		var errmsg string
		var node sqlparser.Statement

		if test.sql != "" {
			node, err = sqlparser.Parse(test.sql)
			assert.Nil(t, err)
		}
		err = handler.Check(test.db, test.user, node)
		log.Debug("err:%v", err)
		if err != nil {
			errmsg = err.Error()
		}
		assert.True(t, errmsg == test.err)
	}
}

func TestLoadUserPrivilegeDenied(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.ERROR))

	// Init privileges.
	{
		// userRS.
		for i := 2; i < len(userRs.Fields); i++ {
			userRs.Rows[0][i] = sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("N"))
		}

		// dbRs.
		i := 2
		for ; i < len(userRs.Fields); i++ {
			dbRs.Rows[0][i] = sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("N"))
		}
		dbRs.Rows[0][i] = sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test"))
	}

	// Create scatter and query handler.
	fakedbs, proxy, cleanup := proxy.MockProxy(log)
	defer cleanup()
	scatter := proxy.Scatter()

	fakedbs.AddQuery("select host, user, select_priv, insert_priv, update_priv, delete_priv, create_priv, drop_priv, alter_priv, index_priv, show_db_priv, super_priv from mysql.user", userRs)
	fakedbs.AddQueryPattern("select host, user, select_priv, insert_priv, update_priv, delete_priv, create_priv, drop_priv, grant_priv, alter_priv, index_priv, db from .*", dbRs)

	handler := NewPrivilege(log, nil, scatter)
	err := handler.Init()
	assert.Nil(t, err)
	defer handler.Close()

	tests := []struct {
		name string
		db   string
		user string
		sql  string
		err  string
	}{
		{
			name: "select.denied",
			db:   "test",
			user: "mock",
			sql:  "select * from test1.t1",
			err:  "Access denied for user 'mock'@'test1' (errno 1045) (sqlstate 28000)",
		},

		{
			name: "show.denied",
			db:   "test",
			user: "mock",
			sql:  "show table status",
			err:  "Access denied for user 'mock'@'test' (errno 1045) (sqlstate 28000)",
		},
	}

	for _, test := range tests {
		var err error
		var node sqlparser.Statement

		if test.sql != "" {
			node, err = sqlparser.Parse(test.sql)
			assert.Nil(t, err)
		}
		err = handler.Check(test.db, test.user, node)
		if err != nil {
			assert.Equal(t, err.Error(), test.err)
		}
	}
}
