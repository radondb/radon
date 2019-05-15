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

	"backend"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestLoadUserPrivilege(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	// Create scatter and query handler.
	scatter, fakedbs, cleanup := backend.MockScatter(log, 3)
	defer cleanup()

	MockInitPrivilegeY(fakedbs)

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
			err:  "Access denied for user 'mock1'@'%' to database 'test' (errno 1045) (sqlstate 28000)",
		},

		{
			name: "user.not.exists",
			db:   "test",
			user: "mock1",
			sql:  "",
			err:  "Access denied for user 'mock1'@'%' to database 'test' (errno 1045) (sqlstate 28000)",
		},
	}

	for i, test := range tests {
		var err error
		var errmsg string
		var node sqlparser.Statement

		if test.sql != "" {
			node, err = sqlparser.Parse(test.sql)
			assert.Nil(t, err)
		}
		err = handler.Check(test.db, test.user, node)
		log.Warning("err:%v, i:%d", err, i)
		if err != nil {
			errmsg = err.Error()
		}
		assert.EqualValues(t, test.err, errmsg)
	}
}

func TestLoadUserPrivilegeDenied(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.ERROR))

	// Create scatter and query handler.
	scatter, fakedbs, cleanup := backend.MockScatter(log, 4)
	defer cleanup()

	MockInitPrivilegeN(fakedbs)

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
			err:  "Access denied for user 'mock'@'%' to database 'test1' (errno 1045) (sqlstate 28000)",
		},

		{
			name: "show.denied",
			db:   "test",
			user: "mock",
			sql:  "show table status",
			err:  "Access denied for user 'mock'@'%' to database 'test' (errno 1045) (sqlstate 28000)",
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
		assert.NotNil(t, err)
		if err != nil {
			assert.Equal(t, err.Error(), test.err)
		}
	}
}

func TestIsSuperPriv(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.ERROR))

	// Create scatter and query handler.
	scatter, fakedbs, cleanup := backend.MockScatter(log, 2)
	defer cleanup()

	MockInitPrivilegeY(fakedbs)

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
			name: "show.processlist.ok",
			db:   "test",
			user: "mock",
			sql:  "SHOW PROCESSLIST",
			err:  "",
		},
	}

	for _, test := range tests {
		isSuper := handler.IsSuperPriv(test.user)
		assert.Equal(t, true, isSuper)
	}
}

func TestGetUserPrivilegeDB(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	// Create scatter and query handler.
	scatter, fakedbs, cleanup := backend.MockScatter(log, 2)
	defer cleanup()

	MockInitPrivilegeNotSuper(fakedbs)

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
			name: "show.databases.ok",
			db:   "test",
			user: "mock",
			sql:  "SHOW DATABASES",
			err:  "",
		},
		{
			name: "show.databases.ok",
			db:   "test1",
			user: "mock",
			sql:  "SHOW DATABASES",
			err:  "",
		},
	}

	for _, test := range tests[:1] {
		dbs := handler.GetUserPrivilegeDBS(test.user)
		_, ok := dbs[test.db]
		assert.Equal(t, true, ok)
	}

	for _, test := range tests[:1] {
		isExist := handler.CheckDBinUserPrivilege(test.user, test.db)
		assert.Equal(t, true, isExist)
	}

	for _, test := range tests[1:2] {
		isExist := handler.CheckDBinUserPrivilege(test.user, test.db)
		assert.Equal(t, false, isExist)
	}

	for _, test := range tests {
		isSet := handler.CheckUserPrivilegeIsSet(test.user)
		assert.EqualValues(t, true, isSet)
	}
}

func TestCheckPrivilegeSkipCol(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))

	// Create scatter and query handler.
	scatter, fakedbs, cleanup := backend.MockScatter(log, 3)
	defer cleanup()

	MockInitPrivilegeUserNDatabaseY(fakedbs)

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
			db:   "",
			user: "mock",
			sql:  "select * from test.t1",
			err:  "",
		},
		{
			name: "select.ok",
			db:   "",
			user: "mock",
			sql:  "select t1.a from test.t1",
			err:  "",
		},
		{
			name: "select.ok",
			db:   "",
			user: "mock",
			sql:  "select a.c1 from test.t1 as a",
			err:  "",
		},
	}

	for i, test := range tests {
		var err error
		var errmsg string
		var node sqlparser.Statement

		if test.sql != "" {
			node, err = sqlparser.Parse(test.sql)
			assert.Nil(t, err)
		}
		err = handler.Check(test.db, test.user, node)
		log.Warning("err:%v, i:%d", err, i)
		if err != nil {
			errmsg = err.Error()
		}
		assert.EqualValues(t, test.err, errmsg)
	}
}

func TestBackendNull(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.ERROR))

	// Create scatter and query handler.
	scatter, fakedbs, cleanup := backend.MockScatter(log, 0)
	defer cleanup()

	MockInitPrivilegeY(fakedbs)

	handler := NewPrivilege(log, nil, scatter)
	err := handler.Init()
	assert.Nil(t, err)
	defer handler.Close()
}
