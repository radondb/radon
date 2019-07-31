package replication

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/stretchr/testify/assert"
)

// Use docker mysql to test, mysql is 3306, mariadb is 3316
var testHost = flag.String("host", "127.0.0.1", "MySQL master host")

var testOutputLogs = flag.Bool("out", false, "output binlog event")

type testSyncerSuite struct {
	b *BinlogSyncer
	c *client.Conn

	wg sync.WaitGroup

	flavor string
}

func testExecute(t *testing.T, query string, syn *testSyncerSuite) {
	_, err := syn.c.Execute(query)
	assert.Nil(t, err)
}

func testSync(t *testing.T, s *BinlogStreamer, syn *testSyncerSuite) {
	syn.wg.Add(1)
	go func() {
		defer syn.wg.Done()

		if s == nil {
			return
		}

		eventCount := 0
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			e, err := s.GetEvent(ctx)
			cancel()

			if err == context.DeadlineExceeded {
				eventCount += 1
				return
			}

			assert.Nil(t, err)

			if *testOutputLogs {
				e.Dump(os.Stdout)
				os.Stdout.Sync()
			}
		}
	}()

	//use mixed format
	testExecute(t, "SET SESSION binlog_format = 'MIXED'", syn)

	str := `DROP TABLE IF EXISTS test_replication`
	testExecute(t, str, syn)

	str = `CREATE TABLE test_replication (
			id BIGINT(64) UNSIGNED  NOT NULL AUTO_INCREMENT,
			str VARCHAR(256),
			f FLOAT,
			d DOUBLE,
			de DECIMAL(10,2),
			i INT,
			bi BIGINT,
			e enum ("e1", "e2"),
			b BIT(8),
			y YEAR,
			da DATE,
			ts TIMESTAMP,
			dt DATETIME,
			tm TIME,
			t TEXT,
			bb BLOB,
			se SET('a', 'b', 'c'),
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8`

	testExecute(t, str, syn)

	//use row format
	testExecute(t, "SET SESSION binlog_format = 'ROW'", syn)

	testExecute(t, `INSERT INTO test_replication (str, f, i, e, b, y, da, ts, dt, tm, de, t, bb, se)
		VALUES ("3", -3.14, 10, "e1", 0b0011, 1985,
		"2012-05-07", "2012-05-07 14:01:01", "2012-05-07 14:01:01",
		"14:01:01", -45363.64, "abc", "12345", "a,b")`, syn)

	id := 100

	if syn.flavor == mysql.MySQLFlavor {
		testExecute(t, "SET SESSION binlog_row_image = 'MINIMAL'", syn)

		testExecute(t, fmt.Sprintf(`INSERT INTO test_replication (id, str, f, i, bb, de) VALUES (%d, "4", -3.14, 100, "abc", -45635.64)`, id), syn)
		testExecute(t, fmt.Sprintf(`UPDATE test_replication SET f = -12.14, de = 555.34 WHERE id = %d`, id), syn)
		testExecute(t, fmt.Sprintf(`DELETE FROM test_replication WHERE id = %d`, id), syn)
	}

	// check whether we can create the table including the json field
	str = `DROP TABLE IF EXISTS test_json`
	testExecute(t, str, syn)

	str = `CREATE TABLE test_json (
			id BIGINT(64) UNSIGNED  NOT NULL AUTO_INCREMENT,
			c1 JSON,
			c2 DECIMAL(10, 0),
			PRIMARY KEY (id)
			) ENGINE=InnoDB`

	if _, err := syn.c.Execute(str); err == nil {
		testExecute(t, `INSERT INTO test_json (c2) VALUES (1)`, syn)
		testExecute(t, `INSERT INTO test_json (c1, c2) VALUES ('{"key1": "value1", "key2": "value2"}', 1)`, syn)
	}

	testExecute(t, "DROP TABLE IF EXISTS test_json_v2", syn)

	str = `CREATE TABLE test_json_v2 (
			id INT, 
			c JSON, 
			PRIMARY KEY (id)
			) ENGINE=InnoDB`

	if _, err := syn.c.Execute(str); err == nil {
		tbls := []string{
			// Refer: https://github.com/shyiko/mysql-binlog-connector-java/blob/c8e81c879710dc19941d952f9031b0a98f8b7c02/src/test/java/com/github/shyiko/mysql/binlog/event/deserialization/json/JsonBinaryValueIntegrationTest.java#L84
			// License: https://github.com/shyiko/mysql-binlog-connector-java#license
			`INSERT INTO test_json_v2 VALUES (0, NULL)`,
			`INSERT INTO test_json_v2 VALUES (1, '{\"a\": 2}')`,
			`INSERT INTO test_json_v2 VALUES (2, '[1,2]')`,
			`INSERT INTO test_json_v2 VALUES (3, '{\"a\":\"b\", \"c\":\"d\",\"ab\":\"abc\", \"bc\": [\"x\", \"y\"]}')`,
			`INSERT INTO test_json_v2 VALUES (4, '[\"here\", [\"I\", \"am\"], \"!!!\"]')`,
			`INSERT INTO test_json_v2 VALUES (5, '\"scalar string\"')`,
			`INSERT INTO test_json_v2 VALUES (6, 'true')`,
			`INSERT INTO test_json_v2 VALUES (7, 'false')`,
			`INSERT INTO test_json_v2 VALUES (8, 'null')`,
			`INSERT INTO test_json_v2 VALUES (9, '-1')`,
			`INSERT INTO test_json_v2 VALUES (10, CAST(CAST(1 AS UNSIGNED) AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (11, '32767')`,
			`INSERT INTO test_json_v2 VALUES (12, '32768')`,
			`INSERT INTO test_json_v2 VALUES (13, '-32768')`,
			`INSERT INTO test_json_v2 VALUES (14, '-32769')`,
			`INSERT INTO test_json_v2 VALUES (15, '2147483647')`,
			`INSERT INTO test_json_v2 VALUES (16, '2147483648')`,
			`INSERT INTO test_json_v2 VALUES (17, '-2147483648')`,
			`INSERT INTO test_json_v2 VALUES (18, '-2147483649')`,
			`INSERT INTO test_json_v2 VALUES (19, '18446744073709551615')`,
			`INSERT INTO test_json_v2 VALUES (20, '18446744073709551616')`,
			`INSERT INTO test_json_v2 VALUES (21, '3.14')`,
			`INSERT INTO test_json_v2 VALUES (22, '{}')`,
			`INSERT INTO test_json_v2 VALUES (23, '[]')`,
			`INSERT INTO test_json_v2 VALUES (24, CAST(CAST('2015-01-15 23:24:25' AS DATETIME) AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (25, CAST(CAST('23:24:25' AS TIME) AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (125, CAST(CAST('23:24:25.12' AS TIME(3)) AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (225, CAST(CAST('23:24:25.0237' AS TIME(3)) AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (26, CAST(CAST('2015-01-15' AS DATE) AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (27, CAST(TIMESTAMP'2015-01-15 23:24:25' AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (127, CAST(TIMESTAMP'2015-01-15 23:24:25.12' AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (227, CAST(TIMESTAMP'2015-01-15 23:24:25.0237' AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (327, CAST(UNIX_TIMESTAMP('2015-01-15 23:24:25') AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (28, CAST(ST_GeomFromText('POINT(1 1)') AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (29, CAST('[]' AS CHAR CHARACTER SET 'ascii'))`,
			// TODO: 30 and 31 are BIT type from JSON_TYPE, may support later.
			`INSERT INTO test_json_v2 VALUES (30, CAST(x'cafe' AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (31, CAST(x'cafebabe' AS JSON))`,
			`INSERT INTO test_json_v2 VALUES (100, CONCAT('{\"', REPEAT('a', 64 * 1024 - 1), '\":123}'))`,
		}

		for _, query := range tbls {
			testExecute(t, query, syn)
		}

		// If MySQL supports JSON, it must supports GEOMETRY.
		testExecute(t, "DROP TABLE IF EXISTS test_geo", syn)

		str = `CREATE TABLE test_geo (g GEOMETRY)`
		_, err = syn.c.Execute(str)
		assert.Nil(t, err)

		tbls = []string{
			`INSERT INTO test_geo VALUES (POINT(1, 1))`,
			`INSERT INTO test_geo VALUES (LINESTRING(POINT(0,0), POINT(1,1), POINT(2,2)))`,
			// TODO: add more geometry tests
		}

		for _, query := range tbls {
			testExecute(t, query, syn)
		}
	}

	str = `DROP TABLE IF EXISTS test_parse_time`
	testExecute(t, str, syn)

	// Must allow zero time.
	testExecute(t, `SET sql_mode=''`, syn)
	str = `CREATE TABLE test_parse_time (
			a1 DATETIME, 
			a2 DATETIME(3), 
			a3 DATETIME(6), 
			b1 TIMESTAMP, 
			b2 TIMESTAMP(3) , 
			b3 TIMESTAMP(6))`
	testExecute(t, str, syn)

	testExecute(t, `INSERT INTO test_parse_time VALUES
		("2014-09-08 17:51:04.123456", "2014-09-08 17:51:04.123456", "2014-09-08 17:51:04.123456", 
		"2014-09-08 17:51:04.123456","2014-09-08 17:51:04.123456","2014-09-08 17:51:04.123456"),
		("0000-00-00 00:00:00.000000", "0000-00-00 00:00:00.000000", "0000-00-00 00:00:00.000000",
		"0000-00-00 00:00:00.000000", "0000-00-00 00:00:00.000000", "0000-00-00 00:00:00.000000"),
		("2014-09-08 17:51:04.000456", "2014-09-08 17:51:04.000456", "2014-09-08 17:51:04.000456", 
		"2014-09-08 17:51:04.000456","2014-09-08 17:51:04.000456","2014-09-08 17:51:04.000456")`, syn)

	syn.wg.Wait()
}

func setupTest(t *testing.T, flavor string, syn *testSyncerSuite) {
	var port uint16 = 3306
	switch flavor {
	case mysql.MariaDBFlavor:
		port = 3316
	}

	syn.flavor = flavor

	var err error
	if syn.c != nil {
		syn.c.Close()
	}

	syn.c, err = client.Connect(fmt.Sprintf("%s:%d", *testHost, port), "root", "", "")
	if err != nil {
		t.Skip(err.Error())
	}

	// _, err = t.c.Execute("CREATE DATABASE IF NOT EXISTS test")
	// c.Assert(err, IsNil)

	_, err = syn.c.Execute("USE test")
	assert.Nil(t, err)

	if syn.b != nil {
		syn.b.Close()
	}

	cfg := BinlogSyncerConfig{
		ServerID:   100,
		Flavor:     flavor,
		Host:       *testHost,
		Port:       port,
		User:       "root",
		Password:   "",
		UseDecimal: true,
	}

	syn.b = NewBinlogSyncer(cfg)
}

func testPositionSync(t *testing.T, syn *testSyncerSuite) {
	//get current master binlog file and position
	r, err := syn.c.Execute("SHOW MASTER STATUS")
	assert.Nil(t, err)
	binFile, _ := r.GetString(0, 0)
	binPos, _ := r.GetInt(0, 1)

	s, err := syn.b.StartSync(mysql.Position{Name: binFile, Pos: uint32(binPos)})
	assert.Nil(t, err)

	// Test re-sync.
	time.Sleep(100 * time.Millisecond)
	syn.b.c.SetReadDeadline(time.Now().Add(time.Millisecond))
	time.Sleep(100 * time.Millisecond)

	testSync(t, s, syn)
}

func TestMysqlPositionSync(t *testing.T) {
	var syn = &testSyncerSuite{}
	setupTest(t, mysql.MySQLFlavor, syn)
	defer tearDownTest(syn)
	testPositionSync(t, syn)
}

func TestMysqlGTIDSync(t *testing.T) {
	var syn = &testSyncerSuite{}
	setupTest(t, mysql.MySQLFlavor, syn)
	defer tearDownTest(syn)

	r, err := syn.c.Execute("SELECT @@gtid_mode")
	assert.Nil(t, err)
	modeOn, _ := r.GetString(0, 0)
	if modeOn != "ON" {
		t.Skip("GTID mode is not ON")
	}

	r, err = syn.c.Execute("SHOW GLOBAL VARIABLES LIKE 'SERVER_UUID'")
	assert.Nil(t, err)

	var masterUuid uuid.UUID
	if s, _ := r.GetString(0, 1); len(s) > 0 && s != "NONE" {
		masterUuid, err = uuid.FromString(s)
		assert.Nil(t, err)
	}

	set, _ := mysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:%d-%d", masterUuid.String(), 1, 2))

	s, err := syn.b.StartSyncGTID(set)
	assert.Nil(t, err)

	testSync(t, s, syn)
}

func TestMariadbPositionSync(t *testing.T) {
	var syn = &testSyncerSuite{}
	setupTest(t, mysql.MariaDBFlavor, syn)
	defer tearDownTest(syn)

	testPositionSync(t, syn)
}

func TestMariadbGTIDSync(t *testing.T) {
	var syn = &testSyncerSuite{}
	setupTest(t, mysql.MariaDBFlavor, syn)
	defer tearDownTest(syn)

	// get current master gtid binlog pos
	r, err := syn.c.Execute("SELECT @@gtid_binlog_pos")
	assert.Nil(t, err)

	str, _ := r.GetString(0, 0)
	set, _ := mysql.ParseMariadbGTIDSet(str)

	s, err := syn.b.StartSyncGTID(set)
	assert.Nil(t, err)

	testSync(t, s, syn)
}

func TestMariadbAnnotateRows(t *testing.T) {
	var syn = &testSyncerSuite{}
	defer tearDownTest(syn)
	setupTest(t, mysql.MariaDBFlavor, syn)
	syn.b.cfg.DumpCommandFlag = BINLOG_SEND_ANNOTATE_ROWS_EVENT
	testPositionSync(t, syn)
}

func TestMysqlSemiPositionSync(t *testing.T) {
	var syn = &testSyncerSuite{}
	setupTest(t, mysql.MySQLFlavor, syn)
	defer tearDownTest(syn)

	syn.b.cfg.SemiSyncEnabled = true

	testPositionSync(t, syn)
}

func TestMysqlBinlogCodec(t *testing.T) {
	var syn = &testSyncerSuite{}
	setupTest(t, mysql.MySQLFlavor, syn)
	defer tearDownTest(syn)

	testExecute(t, "RESET MASTER", syn)

	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Wait()

	go func() {
		defer wg.Done()

		testSync(t, nil, syn)

		testExecute(t, "FLUSH LOGS", syn)

		testSync(t, nil, syn)
	}()

	binlogDir := "./var"

	os.RemoveAll(binlogDir)

	err := syn.b.StartBackup(binlogDir, mysql.Position{Name: "", Pos: uint32(0)}, 2*time.Second)
	assert.Nil(t, err)

	p := NewBinlogParser()
	p.SetVerifyChecksum(true)

	f := func(e *BinlogEvent) error {
		if *testOutputLogs {
			e.Dump(os.Stdout)
			os.Stdout.Sync()
		}
		return nil
	}

	dir, err := os.Open(binlogDir)
	assert.Nil(t, err)
	defer dir.Close()

	files, err := dir.Readdirnames(-1)
	assert.Nil(t, err)

	for _, file := range files {
		err = p.ParseFile(path.Join(binlogDir, file), 0, f)
		assert.Nil(t, err)
	}
}

func tearDownTest(syn *testSyncerSuite) {
	if syn.b != nil {
		syn.b.Close()
		syn.b = nil
	}

	if syn.c != nil {
		syn.c.Close()
		syn.c = nil
	}
}
