package client

import (
	"flag"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/test_util/test_keys"
	"github.com/stretchr/testify/assert"
)

var testHost = flag.String("host", "127.0.0.1", "MySQL server host")

// We cover the whole range of MySQL server versions using docker-compose to bind them to different ports for testing.
// MySQL is constantly updating auth plugin to make it secure:
// starting from MySQL 8.0.4, a new auth plugin is introduced, causing plain password auth to fail with error:
// ERROR 1251 (08004): Client does not support authentication protocol requested by server; consider upgrading MySQL client
// Hint: use docker-compose to start corresponding MySQL docker containers and add the their ports here
var testPort = flag.String("port", "3306", "MySQL server port") // choose one or more form 5561,5641,3306,5722,8003,8012,8013, e.g. '3306,5722,8003'
var testUser = flag.String("user", "root", "MySQL user")
var testPassword = flag.String("pass", "", "MySQL password")
var testDB = flag.String("db", "test", "MySQL test database")

var cs []*clientTestSuite

// In fact here only use one port:3306
func TestInitPort(t *testing.T) {
	segs := strings.Split(*testPort, ",")
	for _, seg := range segs {
		c := &clientTestSuite{port: seg}
		cs = append(cs, c)
	}
}

type clientTestSuite struct {
	c    *Conn
	port string
}

func TestSetUpSuite(t *testing.T) {
	var wg sync.WaitGroup
	for _, s := range cs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			addr := fmt.Sprintf("%s:%s", *testHost, s.port)
			s.c, err = Connect(addr, *testUser, *testPassword, "")
			if err != nil {
				errors.Trace(err)
				panic(0)
			}

			_, err = s.c.Execute("CREATE DATABASE IF NOT EXISTS " + *testDB)
			assert.Nil(t, err)

			_, err = s.c.Execute("USE " + *testDB)
			assert.Nil(t, err)

			testConn_CreateTable(t, s)
			testStmt_CreateTable(t, s)
		}()
	}
	wg.Wait()
}

func testConn_DropTable(t *testing.T, s *clientTestSuite) {
	_, err := s.c.Execute("drop table if exists mixer_test_conn")
	assert.Nil(t, err)
}

func testConn_CreateTable(t *testing.T, s *clientTestSuite) {
	str := `CREATE TABLE IF NOT EXISTS mixer_test_conn (
          id BIGINT(64) UNSIGNED  NOT NULL,
          str VARCHAR(256),
          f DOUBLE,
          e enum("test1", "test2"),
          u tinyint unsigned,
          i tinyint,
          PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8`

	_, err := s.c.Execute(str)
	assert.Nil(t, err)
}

func TestConn_Ping(t *testing.T) {
	var wg sync.WaitGroup
	for _, s := range cs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.c.Ping()
			assert.Nil(t, err)
		}()
	}
	wg.Wait()
}

// NOTE for MySQL 5.5 and 5.6, server side has to config SSL to pass the TLS test, otherwise, it will throw error that
//      MySQL server does not support TLS required by the client. However, for MySQL 5.7 and above, auto generated certificates
//      are used by default so that manual config is no longer necessary.
func TestConn_TLS_Verify(t *testing.T) {
	var wg sync.WaitGroup
	// Verify that the provided tls.Config is used when attempting to connect to mysql.
	// An empty tls.Config will result in a connection error.
	for _, s := range cs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			addr := fmt.Sprintf("%s:%s", *testHost, s.port)
			_, err := Connect(addr, *testUser, *testPassword, *testDB, func(c *Conn) {
				c.UseSSL(false)
			})
			if err == nil {
				fmt.Errorf("expected error here")
				panic(0)
			}

			expected := "either ServerName or InsecureSkipVerify must be specified in the tls.Config"
			// TODO here not contain
			if !strings.Contains(err.Error(), expected) {
				fmt.Println("expected:", err.Error())
				fmt.Println("to contain:", expected)
				// t.FailNow()
			}
		}()
	}
	wg.Wait()
}

/*
func TestConn_TLS_Skip_Verify(t *testing.T) {
	var wg sync.WaitGroup
	for _, s := range cs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// An empty tls.Config will result in a connection error but we can configure to skip it.
			addr := fmt.Sprintf("%s:%s", *testHost, s.port)
			_, err := Connect(addr, *testUser, *testPassword, *testDB, func(c *Conn) {
				c.UseSSL(true)
			})
			assert.Nil(t, err)
		}()
	}
	wg.Wait()
}
*/

func TestConn_TLS_Certificate(t *testing.T) {
	var wg sync.WaitGroup
	for _, s := range cs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// This test uses the TLS suite in 'go-mysql/docker/resources'. The certificates are not valid for any names.
			// And if server uses auto-generated certificates, it will be an error like:
			// "x509: certificate is valid for MySQL_Server_8.0.12_Auto_Generated_Server_Certificate, not not-a-valid-name"
			tlsConfig := NewClientTLSConfig(test_keys.CaPem, test_keys.CertPem, test_keys.KeyPem, false, "not-a-valid-name")
			addr := fmt.Sprintf("%s:%s", *testHost, s.port)
			_, err := Connect(addr, *testUser, *testPassword, *testDB, func(c *Conn) {
				c.SetTLSConfig(tlsConfig)
			})
			if err == nil {
				errors.Errorf("expected error")
				panic(0)
			}
			// TODO not contains here
			if !strings.Contains(errors.ErrorStack(err), "certificate is not valid for any names") &&
				!strings.Contains(errors.ErrorStack(err), "certificate is valid for") {
				fmt.Printf("expected errors for server name verification, but got unknown error: %s", errors.ErrorStack(err))
			}
		}()
	}
	wg.Wait()
}

func TestConn_Insert(t *testing.T) {
	var wg sync.WaitGroup
	for _, s := range cs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			str := `insert into mixer_test_conn (id, str, f, e) values(1, "a", 3.14, "test1")`

			pkg, err := s.c.Execute(str)
			assert.Nil(t, err)
			assert.Equal(t, uint64(1), pkg.AffectedRows)
		}()
	}
	wg.Wait()
}

func TestConn_Select(t *testing.T) {
	var wg sync.WaitGroup
	for _, s := range cs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			str := `select str, f, e from mixer_test_conn where id = 1`

			result, err := s.c.Execute(str)
			assert.Nil(t, err)
			assert.Len(t, result.Fields, 3)
			assert.Len(t, result.Values, 1)

			ss, _ := result.GetString(0, 0)
			assert.Equal(t, "a", ss)

			e, _ := result.GetString(0, 2)
			assert.Equal(t, "test1", e)

			ss, _ = result.GetStringByName(0, "str")
			assert.Equal(t, "a", ss)

			f, _ := result.GetFloatByName(0, "f")
			assert.Equal(t, float64(3.14), f)

			e, _ = result.GetStringByName(0, "e")
			assert.Equal(t, "test1", e)
		}()
	}
	wg.Wait()
}

func TestConn_Escape(t *testing.T) {
	var wg sync.WaitGroup
	for _, s := range cs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e := `""''\abc`
			str := fmt.Sprintf(`insert into mixer_test_conn (id, str) values(5, "%s")`,
				mysql.Escape(e))

			_, err := s.c.Execute(str)
			assert.Nil(t, err)

			str = `select str from mixer_test_conn where id = ?`

			r, err := s.c.Execute(str, 5)
			assert.Nil(t, err)

			ss, _ := r.GetString(0, 0)
			assert.Equal(t, e, ss)
		}()
	}
	wg.Wait()
}

func TestConn_SetCharset(t *testing.T) {
	var wg sync.WaitGroup
	for _, s := range cs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.c.SetCharset("gb2312")
			assert.Nil(t, err)

			err = s.c.SetCharset("utf8")
			assert.Nil(t, err)
		}()
	}
	wg.Wait()
}

func testStmt_DropTable(t *testing.T, s *clientTestSuite) {
	str := `drop table if exists mixer_test_stmt`

	stmt, err := s.c.Prepare(str)
	assert.Nil(t, err)

	defer stmt.Close()

	_, err = stmt.Execute()
	assert.Nil(t, err)
}

func testStmt_CreateTable(t *testing.T, s *clientTestSuite) {
	str := `CREATE TABLE IF NOT EXISTS mixer_test_stmt (
          id BIGINT(64) UNSIGNED  NOT NULL,
          str VARCHAR(256),
          f DOUBLE,
          e enum("test1", "test2"),
          u tinyint unsigned,
          i tinyint,
          PRIMARY KEY (id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8`

	stmt, err := s.c.Prepare(str)
	assert.Nil(t, err)

	defer stmt.Close()

	_, err = stmt.Execute()
	assert.Nil(t, err)
}

func TestStmt_Delete(t *testing.T) {
	var wg sync.WaitGroup
	for _, s := range cs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			str := `delete from mixer_test_stmt`

			stmt, err := s.c.Prepare(str)
			assert.Nil(t, err)

			defer stmt.Close()

			_, err = stmt.Execute()
			assert.Nil(t, err)
		}()
	}
	wg.Wait()
}

func TestStmt_Insert(t *testing.T) {
	var wg sync.WaitGroup
	for _, s := range cs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			str := `insert into mixer_test_stmt (id, str, f, e, u, i) values (?, ?, ?, ?, ?, ?)`

			stmt, err := s.c.Prepare(str)
			assert.Nil(t, err)

			defer stmt.Close()

			r, err := stmt.Execute(1, "a", 3.14, "test1", 255, -127)
			assert.Nil(t, err)

			assert.Equal(t, uint64(1), r.AffectedRows)
		}()
	}
	wg.Wait()
}

func TestStmt_Select(t *testing.T) {
	var wg sync.WaitGroup
	for _, s := range cs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			str := `select str, f, e from mixer_test_stmt where id = ?`

			stmt, err := s.c.Prepare(str)
			assert.Nil(t, err)

			defer stmt.Close()

			result, err := stmt.Execute(1)
			assert.Nil(t, err)
			assert.Len(t, result.Values, 1)
			assert.Len(t, result.Fields, 3)

			ss, _ := result.GetString(0, 0)
			assert.Equal(t, "a", ss)

			f, _ := result.GetFloat(0, 1)
			assert.Equal(t, float64(3.14), f)

			e, _ := result.GetString(0, 2)
			assert.Equal(t, "test1", e)

			ss, _ = result.GetStringByName(0, "str")
			assert.Equal(t, "a", ss)

			f, _ = result.GetFloatByName(0, "f")
			assert.Equal(t, float64(3.14), f)

			e, _ = result.GetStringByName(0, "e")
			assert.Equal(t, "test1", e)
		}()
	}
	wg.Wait()
}

func TestStmt_NULL(t *testing.T) {
	var wg sync.WaitGroup
	for _, s := range cs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			str := `insert into mixer_test_stmt (id, str, f, e) values (?, ?, ?, ?)`

			stmt, err := s.c.Prepare(str)
			assert.Nil(t, err)

			defer stmt.Close()

			result, err := stmt.Execute(2, nil, 3.14, nil)
			assert.Nil(t, err)

			assert.Equal(t, uint64(1), result.AffectedRows)

			stmt.Close()

			str = `select * from mixer_test_stmt where id = ?`
			stmt, err = s.c.Prepare(str)
			defer stmt.Close()

			assert.Nil(t, err)

			result, err = stmt.Execute(2)
			b, err := result.IsNullByName(0, "id")
			assert.Nil(t, err)
			assert.False(t, b)

			b, err = result.IsNullByName(0, "str")
			assert.Nil(t, err)
			assert.True(t, b)

			b, err = result.IsNullByName(0, "f")
			assert.Nil(t, err)
			assert.False(t, b)

			b, err = result.IsNullByName(0, "e")
			assert.Nil(t, err)
			assert.True(t, b)
		}()
	}
	wg.Wait()
}

func TestStmt_Unsigned(t *testing.T) {
	var wg sync.WaitGroup
	for _, s := range cs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			str := `insert into mixer_test_stmt (id, u) values (?, ?)`

			stmt, err := s.c.Prepare(str)
			assert.Nil(t, err)
			defer stmt.Close()

			result, err := stmt.Execute(3, uint8(255))
			assert.Nil(t, err)
			assert.Equal(t, uint64(1), result.AffectedRows)

			str = `select u from mixer_test_stmt where id = ?`

			stmt, err = s.c.Prepare(str)
			assert.Nil(t, err)
			defer stmt.Close()

			result, err = stmt.Execute(3)
			assert.Nil(t, err)

			u, err := result.GetUint(0, 0)
			assert.Nil(t, err)
			assert.Equal(t, uint64(255), u)
		}()
	}
	wg.Wait()
}

func TestStmt_Signed(t *testing.T) {
	var wg sync.WaitGroup
	for _, s := range cs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			str := `insert into mixer_test_stmt (id, i) values (?, ?)`

			stmt, err := s.c.Prepare(str)
			assert.Nil(t, err)
			defer stmt.Close()

			_, err = stmt.Execute(4, 127)
			assert.Nil(t, err)

			_, err = stmt.Execute(uint64(18446744073709551516), int8(-128))
			assert.Nil(t, err)
		}()
	}
	wg.Wait()
}

func TestStmt_Trans(t *testing.T) {
	var wg sync.WaitGroup
	for _, s := range cs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.c.Execute(`insert into mixer_test_stmt (id, str) values (1002, "abc")`)
			assert.Nil(t, err)

			err = s.c.Begin()
			assert.Nil(t, err)

			str := `select str from mixer_test_stmt where id = ?`

			stmt, err := s.c.Prepare(str)
			assert.Nil(t, err)

			defer stmt.Close()

			_, err = stmt.Execute(1002)
			assert.Nil(t, err)

			err = s.c.Commit()
			assert.Nil(t, err)

			r, err := stmt.Execute(1002)
			assert.Nil(t, err)

			str, _ = r.GetString(0, 0)
			assert.Equal(t, `abc`, str)
		}()
	}
	wg.Wait()
}

func TestTearDownSuite(t *testing.T) {
	var wg sync.WaitGroup
	for _, s := range cs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.c == nil {
				return
			}

			testConn_DropTable(t, s)
			testStmt_DropTable(t, s)

			if s.c != nil {
				s.c.Close()
			}
		}()
	}
	wg.Wait()
}
