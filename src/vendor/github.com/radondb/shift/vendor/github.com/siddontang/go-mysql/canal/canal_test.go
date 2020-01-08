package canal

import (
	"bytes"
	"flag"
	"fmt"
	"testing"
	"time"

	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
)

var testHost = flag.String("host", "127.0.0.1", "MySQL host")

type canalTestSuite struct {
	c *Canal
}

var s = &canalTestSuite{}

func TestSetUpSuite(t *testing.T) {
	cfg := NewDefaultConfig()
	cfg.Addr = fmt.Sprintf("%s:3306", *testHost)
	cfg.User = "root"
	cfg.HeartbeatPeriod = 200 * time.Millisecond
	cfg.ReadTimeout = 300 * time.Millisecond
	cfg.Dump.ExecutionPath = "mysqldump"
	cfg.Dump.TableDB = "test"
	cfg.Dump.Tables = []string{"canal_test"}
	cfg.Dump.Where = "id>0"

	// include & exclude config
	cfg.IncludeTableRegex = make([]string, 1)
	cfg.IncludeTableRegex[0] = ".*\\.canal_test"
	cfg.ExcludeTableRegex = make([]string, 2)
	cfg.ExcludeTableRegex[0] = "mysql\\..*"
	cfg.ExcludeTableRegex[1] = ".*\\..*_inner"

	var err error
	s.c, err = NewCanal(cfg)
	assert.Nil(t, err)
	execute(t, "DROP TABLE IF EXISTS test.canal_test")
	sql := `
        CREATE TABLE IF NOT EXISTS test.canal_test (
			id int AUTO_INCREMENT,
			content blob DEFAULT NULL,
            name varchar(100),
			mi mediumint(8) NOT NULL DEFAULT 0,
			umi mediumint(8) unsigned NOT NULL DEFAULT 0,
            PRIMARY KEY(id)
            )ENGINE=innodb;
    `

	execute(t, sql)

	execute(t, "DELETE FROM test.canal_test")
	execute(t, "INSERT INTO test.canal_test (content, name, mi, umi) VALUES (?, ?, ?, ?), (?, ?, ?, ?), (?, ?, ?, ?)", "1", "a", 0, 0, `\0\ndsfasdf`, "b", 1, 16777215, "", "c", -1, 1)

	execute(t, "SET GLOBAL binlog_format = 'ROW'")

	s.c.SetEventHandler(&testEventHandler{})
	go func() {
		// issue:https://github.com/siddontang/go-mysql/commit/8804d83ea8328534e3c47c0f1bf5a34d8a455a60
		// err = s.c.Run()
		set, _ := mysql.ParseGTIDSet("mysql", "")
		err = s.c.StartFromGTID(set)
		assert.Nil(t, err)
	}()
}

func execute(t *testing.T, query string, args ...interface{}) *mysql.Result {
	r, err := s.c.Execute(query, args...)
	assert.Nil(t, err)
	return r
}

type testEventHandler struct {
	DummyEventHandler
}

func (h *testEventHandler) OnRow(e *RowsEvent) error {
	log.Infof("OnRow %s %v\n", e.Action, e.Rows)
	umi, ok := e.Rows[0][4].(uint32) // 4th col is umi. mysqldump gives uint64 instead of uint32
	if ok && (umi != 0 && umi != 1 && umi != 16777215) {
		return fmt.Errorf("invalid unsigned medium int %d", umi)
	}
	return nil
}

func (h *testEventHandler) String() string {
	return "testEventHandler"
}

func (h *testEventHandler) OnPosSynced(p mysql.Position, set mysql.GTIDSet, f bool) error {
	return nil
}

func TestCanal(t *testing.T) {
	<-s.c.WaitDumpDone()

	for i := 1; i < 10; i++ {
		execute(t, "INSERT INTO test.canal_test (name) VALUES (?)", fmt.Sprintf("%d", i))
	}
	execute(t, "INSERT INTO test.canal_test (mi,umi) VALUES (?,?), (?,?), (?,?)", 0, 0, -1, 16777215, 1, 1)
	execute(t, "ALTER TABLE test.canal_test ADD `age` INT(5) NOT NULL AFTER `name`")
	execute(t, "INSERT INTO test.canal_test (name,age) VALUES (?,?)", "d", "18")

	err := s.c.CatchMasterPos(10 * time.Second)
	assert.Nil(t, err)
}

func TestCanalFilter(t *testing.T) {
	// included
	sch, err := s.c.GetTable("test", "canal_test")
	assert.Nil(t, err)
	assert.NotNil(t, sch)
	_, err = s.c.GetTable("not_exist_db", "canal_test")
	assert.NotEqual(t, ErrExcludedTable, errors.Trace(err))
	// excluded
	sch, err = s.c.GetTable("test", "canal_test_inner")
	assert.Equal(t, ErrExcludedTable, errors.Cause(err))
	assert.Nil(t, sch)
	sch, err = s.c.GetTable("mysql", "canal_test")
	assert.Equal(t, ErrExcludedTable, errors.Cause(err))
	assert.Nil(t, sch)
	sch, err = s.c.GetTable("not_exist_db", "not_canal_test")
	assert.Equal(t, ErrExcludedTable, errors.Cause(err))
	assert.Nil(t, sch)
}

func TestCreateTableExp(t *testing.T) {
	cases := []string{
		"CREATE TABLE `mydb.mytable` (`id` int(10)) ENGINE=InnoDB",
		"CREATE TABLE `mytable` (`id` int(10)) ENGINE=InnoDB",
		"CREATE TABLE IF NOT EXISTS `mytable` (`id` int(10)) ENGINE=InnoDB",
		"CREATE TABLE IF NOT EXISTS mytable (`id` int(10)) ENGINE=InnoDB",
	}
	table := []byte("mytable")
	db := []byte("mydb")
	for _, s := range cases {
		m := expCreateTable.FindSubmatch([]byte(s))
		mLen := len(m)
		if m == nil || !bytes.Equal(m[mLen-1], table) || (len(m[mLen-2]) > 0 && !bytes.Equal(m[mLen-2], db)) {
			t.Fatalf("TestCreateTableExp: case %s failed\n", s)
		}
	}
}

func TestAlterTableExp(t *testing.T) {
	cases := []string{
		"ALTER TABLE `mydb`.`mytable` ADD `field2` DATE  NULL  AFTER `field1`;",
		"ALTER TABLE `mytable` ADD `field2` DATE  NULL  AFTER `field1`;",
		"ALTER TABLE mydb.mytable ADD `field2` DATE  NULL  AFTER `field1`;",
		"ALTER TABLE mytable ADD `field2` DATE  NULL  AFTER `field1`;",
		"ALTER TABLE mydb.mytable ADD field2 DATE  NULL  AFTER `field1`;",
	}

	table := []byte("mytable")
	db := []byte("mydb")
	for _, s := range cases {
		m := expAlterTable.FindSubmatch([]byte(s))
		mLen := len(m)
		if m == nil || !bytes.Equal(m[mLen-1], table) || (len(m[mLen-2]) > 0 && !bytes.Equal(m[mLen-2], db)) {
			t.Fatalf("TestAlterTableExp: case %s failed\n", s)
		}
	}
}

func TestRenameTableExp(t *testing.T) {
	cases := []string{
		"rename table `mydb`.`mytable` to `mydb`.`mytable1`",
		"rename table `mytable` to `mytable1`",
		"rename table mydb.mytable to mydb.mytable1",
		"rename table mytable to mytable1",

		"rename table `mydb`.`mytable` to `mydb`.`mytable2`, `mydb`.`mytable3` to `mydb`.`mytable1`",
		"rename table `mytable` to `mytable2`, `mytable3` to `mytable1`",
		"rename table mydb.mytable to mydb.mytable2, mydb.mytable3 to mydb.mytable1",
		"rename table mytable to mytable2, mytable3 to mytable1",
	}
	table := []byte("mytable")
	db := []byte("mydb")
	for _, s := range cases {
		m := expRenameTable.FindSubmatch([]byte(s))
		mLen := len(m)
		if m == nil || !bytes.Equal(m[mLen-1], table) || (len(m[mLen-2]) > 0 && !bytes.Equal(m[mLen-2], db)) {
			t.Fatalf("TestRenameTableExp: case %s failed\n", s)
		}
	}
}

func TestDropTableExp(t *testing.T) {
	cases := []string{
		"drop table test1",
		"DROP TABLE test1",
		"DROP TABLE test1",
		"DROP table IF EXISTS test.test1",
		"drop table `test1`",
		"DROP TABLE `test1`",
		"DROP table IF EXISTS `test`.`test1`",
		"DROP TABLE `test1` /* generated by server */",
		"DROP table if exists test1",
		"DROP table if exists `test1`",
		"DROP table if exists test.test1",
		"DROP table if exists `test`.test1",
		"DROP table if exists `test`.`test1`",
		"DROP table if exists test.`test1`",
		"DROP table if exists test.`test1`",
	}

	table := []byte("test1")
	for _, s := range cases {
		m := expDropTable.FindSubmatch([]byte(s))
		mLen := len(m)
		if m == nil {
			t.Fatalf("TestDropTableExp: case %s failed\n", s)
			return
		}
		if mLen < 4 {
			t.Fatalf("TestDropTableExp: case %s failed\n", s)
			return
		}
		if !bytes.Equal(m[mLen-1], table) {
			t.Fatalf("TestDropTableExp: case %s failed\n", s)
		}
	}
}

func TearDownSuite(t *testing.T) {
	// To test the heartbeat and read timeout,so need to sleep 1 seconds without data transmission
	log.Infof("Start testing the heartbeat and read timeout")
	time.Sleep(time.Second)

	if s.c != nil {
		s.c.Close()
		s.c = nil
	}
}
