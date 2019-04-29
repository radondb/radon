package privilege

import (
	"fakedb"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

var (
	UserRs = &sqltypes.Result{
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

	DbRs = &sqltypes.Result{
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
				Name: "Grant_priv",
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

// MockInitPrivilege init the Rows.
func MockInitPrivilege(fakedbs *fakedb.DB) {
	{
		// userRS.
		for i := 2; i < len(UserRs.Fields); i++ {
			UserRs.Rows[0] = append(UserRs.Rows[0], sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")))
		}

		// dbRs.
		for i := 2; i < len(UserRs.Fields); i++ {
			DbRs.Rows[0] = append(DbRs.Rows[0], sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")))
		}
		DbRs.Rows[0] = append(DbRs.Rows[0], sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test")))
	}

	fakedbs.AddQuery("select host, user, select_priv, insert_priv, update_priv, delete_priv, create_priv, drop_priv, alter_priv, index_priv, show_db_priv, super_priv from mysql.user", UserRs)
	fakedbs.AddQueryPattern("select host, user, select_priv, insert_priv, update_priv, delete_priv, create_priv, drop_priv, grant_priv, alter_priv, index_priv, db from .*", DbRs)
}
