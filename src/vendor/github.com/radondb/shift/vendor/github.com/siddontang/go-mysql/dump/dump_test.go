package dump

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/siddontang/go-mysql/client"
	"github.com/stretchr/testify/assert"
)

// use docker mysql for test
var host = flag.String("host", "127.0.0.1", "MySQL host")
var port = flag.Int("port", 3306, "MySQL host")

var execution = flag.String("exec", "mysqldump", "mysqldump execution path")

type schemaTestSuite struct {
	conn *client.Conn
	d    *Dumper
}

var s = &schemaTestSuite{}

func TestSetUpSuite(t *testing.T) {
	var err error
	s.conn, err = client.Connect(fmt.Sprintf("%s:%d", *host, *port), "root", "", "")
	assert.Nil(t, err)

	s.d, err = NewDumper(*execution, fmt.Sprintf("%s:%d", *host, *port), "root", "")
	assert.Nil(t, err)
	assert.NotNil(t, s.d)

	s.d.SetCharset("utf8")
	s.d.SetErrOut(os.Stderr)

	_, err = s.conn.Execute("CREATE DATABASE IF NOT EXISTS test1")
	assert.Nil(t, err)

	_, err = s.conn.Execute("CREATE DATABASE IF NOT EXISTS test2")
	assert.Nil(t, err)

	str := `CREATE TABLE IF NOT EXISTS test%d.t%d (
			id int AUTO_INCREMENT,
			name varchar(256),
			PRIMARY KEY(id)
			) ENGINE=INNODB`
	_, err = s.conn.Execute(fmt.Sprintf(str, 1, 1))
	assert.Nil(t, err)

	_, err = s.conn.Execute(fmt.Sprintf(str, 2, 1))
	assert.Nil(t, err)

	_, err = s.conn.Execute(fmt.Sprintf(str, 1, 2))
	assert.Nil(t, err)

	_, err = s.conn.Execute(fmt.Sprintf(str, 2, 2))
	assert.Nil(t, err)

	str = `INSERT INTO test%d.t%d (name) VALUES ("a"), ("b"), ("\\"), ("''")`

	_, err = s.conn.Execute(fmt.Sprintf(str, 1, 1))
	assert.Nil(t, err)

	_, err = s.conn.Execute(fmt.Sprintf(str, 2, 1))
	assert.Nil(t, err)

	_, err = s.conn.Execute(fmt.Sprintf(str, 1, 2))
	assert.Nil(t, err)

	_, err = s.conn.Execute(fmt.Sprintf(str, 2, 2))
	assert.Nil(t, err)
}

func TestDump(t *testing.T) {
	// Using mysql 5.7 can't work, error:
	// 	mysqldump: Error 1412: Table definition has changed,
	// 	please retry transaction when dumping table `test_replication` at row: 0
	// err := s.d.Dump(ioutil.Discard)
	// c.Assert(err, IsNil)

	s.d.AddDatabases("test1", "test2")

	s.d.AddIgnoreTables("test1", "t2")

	err := s.d.Dump(ioutil.Discard)
	assert.Nil(t, err)

	s.d.AddTables("test1", "t1")

	err = s.d.Dump(ioutil.Discard)
	assert.Nil(t, err)
}

type testParseHandler struct {
}

func (h *testParseHandler) BinLog(name string, pos uint64) error {
	return nil
}

func (h *testParseHandler) Data(schema string, table string, values []string) error {
	return nil
}

func TestParseFindTable(t *testing.T) {
	tbl := []struct {
		sql   string
		table string
	}{
		{"INSERT INTO `note` VALUES ('title', 'here is sql: INSERT INTO `table` VALUES (\\'some value\\')');", "note"},
		{"INSERT INTO `note` VALUES ('1', '2', '3');", "note"},
		{"INSERT INTO `a.b` VALUES ('1');", "a.b"},
	}

	for _, tx := range tbl {
		res := valuesExp.FindAllStringSubmatch(tx.sql, -1)[0][1]
		assert.Equal(t, tx.table, res)
	}
}

func TestUnescape(t *testing.T) {
	tbl := []struct {
		escaped  string
		expected string
	}{
		{`\\n`, `\n`},
		{`\\t`, `\t`},
		{`\\"`, `\"`},
		{`\\'`, `\'`},
		{`\\0`, `\0`},
		{`\\b`, `\b`},
		{`\\Z`, `\Z`},
		{`\\r`, `\r`},
		{`abc`, `abc`},
		{`abc\`, `abc`},
		{`ab\c`, `abc`},
		{`\abc`, `abc`},
	}

	for _, tx := range tbl {
		unesacped := unescapeString(tx.escaped)
		assert.Equal(t, tx.expected, unesacped)
	}
}

func TestParse(t *testing.T) {
	var buf bytes.Buffer

	s.d.Reset()

	s.d.AddDatabases("test1", "test2")

	err := s.d.Dump(&buf)
	assert.Nil(t, err)

	err = Parse(&buf, new(testParseHandler), true)
	assert.Nil(t, err)
}

func TestParseValue(t *testing.T) {
	str := `'abc\\',''`
	values, err := parseValues(str)
	assert.Nil(t, err)
	assert.Equal(t, []string{`'abc\'`, `''`}, values)

	str = `123,'\Z#÷QÎx£. Æ‘ÇoPâÅ_\r—\\','','qn'`
	values, err = parseValues(str)
	assert.Nil(t, err)
	assert.Len(t, values, 4)

	str = `123,'\Z#÷QÎx£. Æ‘ÇoPâÅ_\r—\\','','qn\'`
	values, err = parseValues(str)
	assert.NotNil(t, err)
}

func TestParseLine(t *testing.T) {
	lines := []struct {
		line     string
		expected string
	}{
		{line: "INSERT INTO `test` VALUES (1, 'first', 'hello mysql; 2', 'e1', 'a,b');",
			expected: "1, 'first', 'hello mysql; 2', 'e1', 'a,b'"},
		{line: "INSERT INTO `test` VALUES (0x22270073646661736661736466, 'first', 'hello mysql; 2', 'e1', 'a,b');",
			expected: "0x22270073646661736661736466, 'first', 'hello mysql; 2', 'e1', 'a,b'"},
	}

	f := func(c rune) bool {
		return c == '\r' || c == '\n'
	}

	for _, tl := range lines {
		l := strings.TrimRightFunc(tl.line, f)

		m := valuesExp.FindAllStringSubmatch(l, -1)

		assert.Len(t, m, 1)
		assert.Regexp(t, m[0][1], "test")
		assert.Regexp(t, m[0][2], tl.expected)
	}
}

func TestTearDownSuite(t *testing.T) {
	if s.conn != nil {
		_, err := s.conn.Execute("DROP DATABASE IF EXISTS test1")
		assert.Nil(t, err)

		_, err = s.conn.Execute("DROP DATABASE IF EXISTS test2")
		assert.Nil(t, err)

		s.conn.Close()
	}
}
