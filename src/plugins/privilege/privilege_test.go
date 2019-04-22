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
	"github.com/xelabs/go-mysqlstack/xlog"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

func TestLoadUserPrivilege(t *testing.T) {
	r1 := &sqltypes.Result{
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
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("bohu")),
				sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")),
				sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")),
				sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")),
				sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")),
				sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")),
				sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")),
				sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")),
				sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")),
				sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")),
				sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")),
			},
		},
	}

	r2 := &sqltypes.Result{
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
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("bohu")),
				sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")),
				sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")),
				sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")),
				sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")),
				sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")),
				sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")),
				sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")),
				sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test")),
			},
		},
	}

	log := xlog.NewStdLog(xlog.Level(xlog.DEBUG))

	// Create scatter and query handler.
	scatter, fakedbs, cleanup := backend.MockScatter(log, 10)
	defer cleanup()
	fakedbs.AddQuery("select host, user, select_priv, insert_priv, update_priv, delete_priv, create_priv, drop_priv, alter_priv, index_priv, show_db_priv, super_priv from mysql.user", r1)
	fakedbs.AddQueryPattern("select host, user, select_priv, insert_priv, update_priv, delete_priv, create_priv, drop_priv, grant_priv, alter_priv, index_priv, db from .*", r2)

	handler := NewPrivilege(log, nil, scatter)
	err := handler.Init()
	assert.Nil(t, err)
}
