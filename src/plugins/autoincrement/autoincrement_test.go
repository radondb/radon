/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package autoincrement

import (
	"testing"

	"config"
	"router"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestPluginAutoincGetAutoIncrement(t *testing.T) {
	tests := []struct {
		query  string
		result *config.AutoIncrement
	}{
		{
			"drop table t1",
			nil,
		},
		{
			"create table t1(a int)",
			nil,
		},
		{
			"create table tab_auto_incr(a int not null auto_increment,b int not null,primary key (a))",
			&config.AutoIncrement{Column: "a"},
		},
	}

	for _, test := range tests {
		node, err := sqlparser.Parse(test.query)
		assert.Nil(t, err)
		ddl := node.(*sqlparser.DDL)
		auto := GetAutoIncrement(ddl)
		if test.result != nil {
			assert.Equal(t, test.result, auto)
		}
	}
}

func TestPluginAutoincModifyForAutoinc(t *testing.T) {
	tests := []struct {
		query   string
		want    string
		autoinc *config.AutoIncrement
	}{
		// No autoinc column.
		{
			query:   "insert into t1(b) values(1),(2),(3)",
			want:    "insert into t1(b, a) values (1, 65536), (2, 65537), (3, 65538)",
			autoinc: &config.AutoIncrement{Column: "a"},
		},

		{
			query:   "insert into t1(b) values(1)",
			want:    "insert into t1(b, a) values (1, 65536)",
			autoinc: &config.AutoIncrement{Column: "a"},
		},

		// replace
		{
			query:   "replace into t1 (b) values(1),(2)",
			want:    "replace into t1(b, a) values (1, 65536), (2, 65537)",
			autoinc: &config.AutoIncrement{Column: "a"},
		},

		// With autoinc column.
		{
			query:   "insert into t1(a) values(1),(2),(3)",
			want:    "insert into t1(a) values (1), (2), (3)",
			autoinc: &config.AutoIncrement{Column: "a"},
		},

		// Insert with select.
		{
			query:   "insert into t1(a) select a from t1",
			want:    "insert into t1(a) select a from t1",
			autoinc: &config.AutoIncrement{Column: "a"},
		},
	}

	for _, test := range tests {
		node, err := sqlparser.Parse(test.query)
		assert.Nil(t, err)
		insert := node.(*sqlparser.Insert)
		modifyForAutoinc(insert, test.autoinc, 65535)

		buf := sqlparser.NewTrackedBuffer(nil)
		insert.Format(buf)
		assert.Equal(t, test.want, buf.String())
	}
}

func TestPluginAutoIncrement(t *testing.T) {
	db := "db1"
	log := xlog.NewStdLog(xlog.Level(xlog.DEBUG))

	// Router.
	route, cleanup := router.MockNewRouter(log)
	defer cleanup()

	// Plugin.
	autoplug := NewAutoIncrement(log, route)
	err := autoplug.Init()
	assert.Nil(t, err)
	defer autoplug.Close()

	tests := []struct {
		query   string
		want    string
		tblconf *config.TableConfig
	}{
		{
			query: "insert into t1(b) values(1),(2),(3)",
			want:  "insert into db1.t1(b, a)",
			tblconf: &config.TableConfig{
				Name:          "t1",
				ShardType:     "GLOBAL",
				AutoIncrement: &config.AutoIncrement{Column: "id"},
			},
		},
		{
			query: "insert into db1.t2(b) values(1),(2),(3)",
			want:  "insert into db1.t2(b, a)",
			tblconf: &config.TableConfig{
				Name:          "t2",
				ShardType:     "GLOBAL",
				AutoIncrement: &config.AutoIncrement{Column: "id"},
			},
		},
		{
			query: "insert into db1.t3(b) values(1),(2),(3)",
			want:  "insert into db1.t3(b, a)",
			tblconf: &config.TableConfig{
				Name:      "t3",
				ShardType: "GLOBAL",
			},
		},
	}

	for _, test := range tests {
		// Add router table config.
		route.AddForTest(db, test.tblconf)

		// Parse.
		node, err := sqlparser.Parse(test.query)
		assert.Nil(t, err)
		insert := node.(*sqlparser.Insert)
		err = autoplug.Process(db, insert)
		assert.Nil(t, err)

		// Check.
		buf := sqlparser.NewTrackedBuffer(nil)
		insert.Format(buf)
		log.Debug("%v", buf.String())
	}
}
