package schema

import (
	"database/sql"
	"flag"
	"fmt"
	"testing"

	"github.com/siddontang/go-mysql/client"
	_ "github.com/siddontang/go-mysql/driver"
	"github.com/stretchr/testify/assert"
)

// use docker mysql for test
var host = flag.String("host", "127.0.0.1", "MySQL host")

type schemaTestSuite struct {
	conn  *client.Conn
	sqlDB *sql.DB
}

var s = &schemaTestSuite{}

// init var s
func TestSetUpSuite(t *testing.T) {
	var err error
	s.conn, err = client.Connect(fmt.Sprintf("%s:%d", *host, 3306), "root", "", "test")
	assert.Nil(t, err)

	s.sqlDB, err = sql.Open("mysql", fmt.Sprintf("root:@%s:3306", *host))
	assert.Nil(t, err)
}

func TestSchema(t *testing.T) {
	_, err := s.conn.Execute(`DROP TABLE IF EXISTS schema_test`)
	assert.Nil(t, err)

	str := `
        CREATE TABLE IF NOT EXISTS schema_test (
            id INT,
            id1 INT,
            id2 INT,
            name VARCHAR(256),
            status ENUM('appointing','serving','abnormal','stop','noaftermarket','finish','financial_audit'),
            se SET('a', 'b', 'c'),
            f FLOAT,
            d DECIMAL(2, 1),
            uint INT UNSIGNED,
            zfint INT ZEROFILL,
            name_ucs VARCHAR(256) CHARACTER SET ucs2,
            name_utf8 VARCHAR(256) CHARACTER SET utf8,
            PRIMARY KEY(id2, id),
            UNIQUE (id1),
            INDEX name_idx (name)
        ) ENGINE = INNODB;
    `

	_, err = s.conn.Execute(str)
	assert.Nil(t, err)

	ta, err := NewTable(s.conn, "test", "schema_test")
	assert.Nil(t, err)

	assert.Len(t, ta.Columns, 12)
	assert.Len(t, ta.Indexes, 3)
	assert.EqualValues(t, []int{2, 0}, ta.PKColumns)
	assert.Len(t, ta.Indexes[0].Columns, 2)
	assert.Equal(t, "PRIMARY", ta.Indexes[0].Name)
	assert.Equal(t, "name_idx", ta.Indexes[2].Name)
	assert.EqualValues(t, ta.Columns[4].EnumValues, []string{"appointing", "serving", "abnormal", "stop", "noaftermarket", "finish", "financial_audit"})
	assert.EqualValues(t, ta.Columns[5].SetValues, []string{"a", "b", "c"})
	assert.Equal(t, ta.Columns[7].Type, TYPE_DECIMAL)
	assert.False(t, ta.Columns[0].IsUnsigned)
	assert.True(t, ta.Columns[8].IsUnsigned)
	assert.True(t, ta.Columns[9].IsUnsigned)
	assert.Regexp(t, "^ucs2.*", ta.Columns[10].Collation)
	assert.Regexp(t, "^utf8.*", ta.Columns[11].Collation)

	taSqlDb, err := NewTableFromSqlDB(s.sqlDB, "test", "schema_test")
	assert.Nil(t, err)

	assert.Equal(t, taSqlDb, ta)
}

func TestQuoteSchema(t *testing.T) {
	str := "CREATE TABLE IF NOT EXISTS `a-b_test` (`a.b` INT) ENGINE = INNODB"

	_, err := s.conn.Execute(str)
	assert.Nil(t, err)

	ta, err := NewTable(s.conn, "test", "a-b_test")
	assert.Nil(t, err)

	assert.Equal(t, ta.Columns[0].Name, "a.b")
}

func TestTearDownSuite(c *testing.T) {
	if s.conn != nil {
		s.conn.Close()
	}

	if s.sqlDB != nil {
		s.sqlDB.Close()
	}
}
