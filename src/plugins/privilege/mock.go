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

	UserRs1 = &sqltypes.Result{
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
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("%")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("mock1")),
			},
		},
	}

	DbRs1 = &sqltypes.Result{
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
			{
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("%")),
				sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("mock")),
			},
		},
	}
)

// MockInitPrivilegeY init the Rows with Y.
func MockInitPrivilegeY(fakedbs *fakedb.DB) {
	UserRs.Rows[0] = UserRs.Rows[0][0:2]
	DbRs.Rows[0] = DbRs.Rows[0][0:2]
	{
		// userRS.
		for i := 2; i < len(UserRs.Fields); i++ {
			UserRs.Rows[0] = append(UserRs.Rows[0], sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")))
		}

		// dbRs.
		for i := 2; i < len(DbRs.Fields)-1; i++ {
			DbRs.Rows[0] = append(DbRs.Rows[0], sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")))
		}
		DbRs.Rows[0] = append(DbRs.Rows[0], sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test")))
	}

	fakedbs.AddQuery("select host, user, select_priv, insert_priv, update_priv, delete_priv, create_priv, drop_priv, alter_priv, index_priv, show_db_priv, super_priv from mysql.user", UserRs)
	fakedbs.AddQueryPattern("select host, user, select_priv, insert_priv, update_priv, delete_priv, create_priv, drop_priv, grant_priv, alter_priv, index_priv, db from .*", DbRs)
}

// MockInitPrivilegeN init the Rows with N.
func MockInitPrivilegeN(fakedbs *fakedb.DB) {
	UserRs.Rows[0] = UserRs.Rows[0][0:2]
	DbRs.Rows[0] = DbRs.Rows[0][0:2]
	{
		// userRS.
		for i := 2; i < len(UserRs.Fields); i++ {
			UserRs.Rows[0] = append(UserRs.Rows[0], sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("N")))
		}

		// dbRs.
		for i := 2; i < len(DbRs.Fields)-1; i++ {
			DbRs.Rows[0] = append(DbRs.Rows[0], sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("N")))
		}
		DbRs.Rows[0] = append(DbRs.Rows[0], sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test")))
	}

	fakedbs.AddQuery("select host, user, select_priv, insert_priv, update_priv, delete_priv, create_priv, drop_priv, alter_priv, index_priv, show_db_priv, super_priv from mysql.user", UserRs)
	fakedbs.AddQueryPattern("select host, user, select_priv, insert_priv, update_priv, delete_priv, create_priv, drop_priv, grant_priv, alter_priv, index_priv, db from .*", DbRs)
}

// MockInitPrivilegeNotSuper init the Rows with N to Super_priv.
func MockInitPrivilegeNotSuper(fakedbs *fakedb.DB) {
	UserRs.Rows[0] = UserRs.Rows[0][0:2]
	DbRs.Rows[0] = DbRs.Rows[0][0:2]
	{
		// userRS.
		for i := 2; i < len(UserRs.Fields)-1; i++ {
			UserRs.Rows[0] = append(UserRs.Rows[0], sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")))
		}
		UserRs.Rows[0] = append(UserRs.Rows[0], sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("N")))

		// dbRs.
		for i := 2; i < len(DbRs.Fields)-1; i++ {
			DbRs.Rows[0] = append(DbRs.Rows[0], sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")))
		}
		DbRs.Rows[0] = append(DbRs.Rows[0], sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test")))
	}

	fakedbs.AddQuery("select host, user, select_priv, insert_priv, update_priv, delete_priv, create_priv, drop_priv, alter_priv, index_priv, show_db_priv, super_priv from mysql.user", UserRs)
	fakedbs.AddQueryPattern("select host, user, select_priv, insert_priv, update_priv, delete_priv, create_priv, drop_priv, grant_priv, alter_priv, index_priv, db from .*", DbRs)
}

// MockInitPrivilegeUsers init the Rows with multiple users.
func MockInitPrivilegeUsers(fakedbs *fakedb.DB) {
	UserRs1.Rows[0] = UserRs1.Rows[0][0:2]
	DbRs1.Rows[0] = DbRs1.Rows[0][0:2]
	{
		// UserRs1.
		for i := 2; i < len(UserRs1.Fields)-1; i++ {
			UserRs1.Rows[0] = append(UserRs1.Rows[0], sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")))
		}
		UserRs1.Rows[0] = append(UserRs1.Rows[0], sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("N")))

		// DbRs1.
		for i := 2; i < len(DbRs1.Fields)-1; i++ {
			DbRs1.Rows[0] = append(DbRs1.Rows[0], sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")))
		}
		DbRs1.Rows[0] = append(DbRs1.Rows[0], sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test")))
	}

	UserRs1.Rows[1] = UserRs1.Rows[1][0:2]
	DbRs1.Rows[1] = DbRs1.Rows[1][0:2]
	{
		// UserRs1.
		for i := 2; i < len(UserRs1.Fields)-1; i++ {
			UserRs1.Rows[1] = append(UserRs1.Rows[1], sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")))
		}
		UserRs1.Rows[1] = append(UserRs1.Rows[1], sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("N")))

		// DbRs1.
		for i := 2; i < len(DbRs1.Fields)-1; i++ {
			DbRs1.Rows[1] = append(DbRs1.Rows[1], sqltypes.MakeTrusted(querypb.Type_ENUM, []byte("Y")))
		}
		DbRs1.Rows[1] = append(DbRs1.Rows[1], sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte("test")))
	}

	fakedbs.AddQuery("select host, user, select_priv, insert_priv, update_priv, delete_priv, create_priv, drop_priv, alter_priv, index_priv, show_db_priv, super_priv from mysql.user", UserRs1)
	fakedbs.AddQueryPattern("select host, user, select_priv, insert_priv, update_priv, delete_priv, create_priv, drop_priv, grant_priv, alter_priv, index_priv, db from .*", DbRs1)
}
