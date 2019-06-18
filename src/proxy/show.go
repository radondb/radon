/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"build"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// handleShowDatabases used to handle the 'SHOW DATABASES' command.
func (spanner *Spanner) handleShowDatabases(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	qr, err := spanner.ExecuteSingle(query)
	if err != nil {
		return nil, err
	}

	privilegePlug := spanner.plugins.PlugPrivilege()
	isSuper := privilegePlug.IsSuperPriv(session.User())
	if isSuper {
		return qr, nil
	} else {
		isSet := privilegePlug.CheckUserPrivilegeIsSet(session.User())
		if isSet {
			return qr, nil
		} else {
			newqr := &sqltypes.Result{}
			for _, row := range qr.Rows {
				db := string(row[0].Raw())
				if isExist := privilegePlug.CheckDBinUserPrivilege(session.User(), db); isExist {
					newqr.RowsAffected++
					newqr.Rows = append(newqr.Rows, row)
				}
			}

			newqr.Fields = []*querypb.Field{
				{Name: "Database", Type: querypb.Type_VARCHAR},
			}
			return newqr, nil
		}
	}
}

// handleShowEngines used to handle the 'SHOW ENGINES' command.
func (spanner *Spanner) handleShowEngines(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	return spanner.ExecuteSingle(query)
}

// handleShowCreateDatabase used to handle the 'SHOW CREATE DATABASE' command.
func (spanner *Spanner) handleShowCreateDatabase(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	return spanner.ExecuteSingle(query)
}

// handleShowTableStatus used to handle the 'SHOW TABLE STATUS' command.
// | Name          | Engine | Version | Row_format  | Rows    | Avg_row_length | Data_length | Max_data_length     | Index_length | Data_free            | Auto_increment | Create_time         | Update_time         | Check_time | Collation       | Checksum | Create_options | Comment |
// +---------------+--------+---------+-------------+---------+----------------+-------------+---------------------+--------------+----------------------+----------------+---------------------+---------------------+------------+-----------------+----------+----------------+---------+
// | block_0000    | TokuDB |      10 | tokudb_zstd |    6134 |           1395 |     8556930 | 9223372036854775807 |       509122 | 18446744073704837574 |           NULL | 2019-04-24 17:36:10 | 2019-05-04 12:47:45 | NULL       | utf8_general_ci |     NULL |                |
func (spanner *Spanner) handleShowTableStatus(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	ast := node.(*sqlparser.Show)
	router := spanner.router

	database := session.Schema()
	if !ast.Database.IsEmpty() {
		database = ast.Database.Name.String()
	}

	if database == "" {
		return nil, sqldb.NewSQLError(sqldb.ER_NO_DB_ERROR)
	}
	// Check the database ACL.
	if err := router.DatabaseACL(database); err != nil {
		return nil, err
	}

	rewritten := fmt.Sprintf("SHOW TABLE STATUS from %s", database)
	qr, err := spanner.ExecuteScatter(rewritten)
	if err != nil {
		return nil, err
	}

	// we will do 6 things to merge the sharding tables to one table.
	// 1. the Name(index: 0) will be removed suffix.
	// 2. the Rows(index: 4) will be accumulated from the value which is estimated in the sharding tables.
	// 3. the Avg_row_length(index: 5) will be the biggest.
	// 4. the Data_length(index: 6) will be accumulated...
	// 5. the Index_length(index: 8) will be accumulated...
	// 6. the Update_time(index: 12) will be the most recent.
	newqr := &sqltypes.Result{}
	tables := make(map[string]*[]sqltypes.Value)
	global := make(map[string]struct{})
	for i, row := range qr.Rows {
		name := string(row[0].Raw())
		// the global table can't be suffixed with _0000.
		var valid = regexp.MustCompile("_[0-9]{4}$")
		Suffix := valid.FindAllStringSubmatch(name, -1)
		var newName string
		if len(Suffix) != 0 {
			newName = strings.TrimSuffix(name, Suffix[0][0])
		} else {
			newName = name
			global[newName] = struct{}{}
		}

		rewrittenRow, ok := tables[newName]
		if !ok {
			row[0] = sqltypes.MakeTrusted(row[0].Type(), []byte(newName))
			newqr.Rows = append(newqr.Rows, row)
			tables[newName] = &qr.Rows[i]
		} else {
			if _, ok = global[newName]; ok {
				continue
			}

			// Rows.
			rows := (*rewrittenRow)[4].ToNative().(uint64) + row[4].ToNative().(uint64)
			if (*rewrittenRow)[4], err = sqltypes.BuildConverted((*rewrittenRow)[4].Type(), rows); err != nil {
				return nil, err
			}

			// Avg_row_length.
			if (*rewrittenRow)[5].ToNative().(uint64) < row[5].ToNative().(uint64) {
				if (*rewrittenRow)[5], err = sqltypes.BuildConverted((*rewrittenRow)[5].Type(), row[5]); err != nil {
					return nil, err
				}
			}

			// Data_length.
			datalength := (*rewrittenRow)[6].ToNative().(uint64) + row[6].ToNative().(uint64)
			if (*rewrittenRow)[6], err = sqltypes.BuildConverted((*rewrittenRow)[6].Type(), datalength); err != nil {
				return nil, err
			}

			// Index_length.
			indexlength := (*rewrittenRow)[8].ToNative().(uint64) + row[8].ToNative().(uint64)
			if (*rewrittenRow)[8], err = sqltypes.BuildConverted((*rewrittenRow)[8].Type(), indexlength); err != nil {
				return nil, err
			}

			var curTime, oldTime time.Time
			switch row[12].Type() {
			case querypb.Type_DATETIME:

				curStr := row[12].String()
				curStr = strings.Replace(curStr, " ", "T", 1)
				curStr = curStr + "Z"
				if curTime, err = time.Parse(time.RFC3339, curStr); err != nil {
					return nil, err
				}
				curSecond := curTime.Unix()

				old := (*rewrittenRow)[12].String()
				if old == "" {
					old = "1970-01-01 00:00:00"
					old = strings.Replace(old, " ", "T", 1)
					old = old + "Z"
				} else {
					old = strings.Replace(old, " ", "T", 1)
					old = old + "Z"
				}

				if oldTime, err = time.Parse(time.RFC3339, old); err != nil {
					return nil, err
				}
				oldSecond := oldTime.Unix()

				if curSecond > oldSecond {
					if (*rewrittenRow)[12], err = sqltypes.BuildConverted((*rewrittenRow)[12].Type(), row[12]); err != nil {
						return nil, err
					}
				}
			}
		}
	}

	len := len(newqr.Rows)
	qr.RowsAffected = uint64(len)
	qr.Rows = qr.Rows[0:0]
	qr.Rows = append(qr.Rows, newqr.Rows...)
	return qr, nil
}

// handleShowTables used to handle the 'SHOW TABLES' command.
func (spanner *Spanner) handleShowTables(session *driver.Session, query string, node *sqlparser.Show) (*sqltypes.Result, error) {
	router := spanner.router
	ast := node

	database := session.Schema()
	if !ast.Database.IsEmpty() {
		database = ast.Database.Name.String()
	}
	if database == "" {
		return nil, sqldb.NewSQLError(sqldb.ER_NO_DB_ERROR)
	}
	// Check the database ACL.
	if err := router.DatabaseACL(database); err != nil {
		return nil, err
	}

	// For validating the query works, we send it to the backend and check the error.
	rewritten := fmt.Sprintf("SHOW TABLES FROM %s", database)
	_, err := spanner.ExecuteScatter(rewritten)
	if err != nil {
		return nil, err
	}

	qr := &sqltypes.Result{}
	tblList := router.Tables()
	tables, ok := tblList[database]
	if ok {
		qr.Fields = []*querypb.Field{
			{Name: fmt.Sprintf("Tables_in_%s", database), Type: querypb.Type_VARCHAR},
		}
		for _, table := range tables {
			row := []sqltypes.Value{sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(table))}
			qr.Rows = append(qr.Rows, row)
		}
	}
	return qr, nil
}

func (spanner *Spanner) handleShowCreateTable(session *driver.Session, query string, node *sqlparser.Show) (*sqltypes.Result, error) {
	router := spanner.router
	ast := node

	table := ast.Table.Name.String()
	database := session.Schema()
	if !ast.Table.Qualifier.IsEmpty() {
		database = ast.Table.Qualifier.String()
	}
	if database == "" {
		return nil, sqldb.NewSQLError(sqldb.ER_NO_DB_ERROR)
	}
	// Check the database ACL.
	if err := router.DatabaseACL(database); err != nil {
		return nil, err
	}

	var qr *sqltypes.Result
	var err error

	shardKey, err := router.ShardKey(database, table)
	if err != nil {
		return nil, err
	}

	// If shardType is GLOBAL or SINGLE, add the tableType to the end of c2;
	// if shardType is HASH, rewrite the query Result.
	if shardKey == "" {
		segments, err := router.Lookup(database, table, nil, nil)
		if err != nil {
			return nil, err
		}
		// single table just on the first backend
		backend := segments[0].Backend

		// If the elapsed > pool.maxIdleTime, the new connection without database, add the database.
		rewritten := fmt.Sprintf("SHOW CREATE TABLE %s.%s", database, table)
		qr, err = spanner.ExecuteOnThisBackend(backend, rewritten)
		if err != nil {
			return nil, err
		}

		tableConfig, err := router.TableConfig(database, table)
		if err != nil {
			return nil, err
		}
		tableType := tableConfig.ShardType
		// 'show create table' has two columns.
		c2 := qr.Rows[0][1]
		// Add tableType to the end of c2Val
		c2Buf := common.NewBuffer(0)
		c2Buf.WriteString(string(c2.Raw()))
		partInfo := fmt.Sprintf("\n/*!%s*/", tableType)
		c2Buf.WriteString(partInfo)
		qr.Rows[0][1] = sqltypes.MakeTrusted(c2.Type(), c2Buf.Datas())
	} else {
		// Get one table from the router.
		parts, err := router.Lookup(database, table, nil, nil)
		if err != nil {
			return nil, err
		}
		partTable := parts[0].Table
		backend := parts[0].Backend
		rewritten := fmt.Sprintf("SHOW CREATE TABLE %s.%s", database, partTable)
		qr, err = spanner.ExecuteOnThisBackend(backend, rewritten)
		if err != nil {
			return nil, err
		}

		// 'show create table' has two columns.
		c1 := qr.Rows[0][0]
		c2 := qr.Rows[0][1]

		// Replace the partition table to raw table.
		c1Val := strings.Replace(string(c1.Raw()), partTable, table, 1)
		c2Val := strings.Replace(string(c2.Raw()), partTable, table, 1)

		// Add partition info to the end of c2Val
		c2Buf := common.NewBuffer(0)
		c2Buf.WriteString(c2Val)
		partInfo := fmt.Sprintf("\n/*!50100 PARTITION BY HASH (%s) */", shardKey)
		c2Buf.WriteString(partInfo)

		qr.Rows[0][0] = sqltypes.MakeTrusted(c1.Type(), []byte(c1Val))
		qr.Rows[0][1] = sqltypes.MakeTrusted(c2.Type(), c2Buf.Datas())
	}
	return qr, nil
}

// handleShowColumns used to handle the 'SHOW COLUMNS' command.
func (spanner *Spanner) handleShowColumns(session *driver.Session, query string, node *sqlparser.Show) (*sqltypes.Result, error) {
	router := spanner.router
	ast := node

	table := ast.Table.Name.String()
	database := session.Schema()
	if !ast.Table.Qualifier.IsEmpty() {
		database = ast.Table.Qualifier.String()
	}
	if database == "" {
		return nil, sqldb.NewSQLError(sqldb.ER_NO_DB_ERROR)
	}
	// Check the database ACL.
	if err := router.DatabaseACL(database); err != nil {
		return nil, err
	}

	// Get one table from the router.
	parts, err := router.Lookup(database, table, nil, nil)
	if err != nil {
		return nil, err
	}
	partTable := parts[0].Table
	backend := parts[0].Backend
	rewritten := fmt.Sprintf("SHOW COLUMNS FROM %s.%s", database, partTable)
	qr, err := spanner.ExecuteOnThisBackend(backend, rewritten)
	if err != nil {
		return nil, err
	}

	return qr, nil
}

// handleShowProcesslist used to handle the query "SHOW PROCESSLIST".
func (spanner *Spanner) handleShowProcesslist(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	sessions := spanner.sessions
	qr := &sqltypes.Result{}
	qr.Fields = []*querypb.Field{
		{Name: "Id", Type: querypb.Type_INT64},
		{Name: "User", Type: querypb.Type_VARCHAR},
		{Name: "Host", Type: querypb.Type_VARCHAR},
		{Name: "db", Type: querypb.Type_VARCHAR},
		{Name: "Command", Type: querypb.Type_VARCHAR},
		{Name: "Time", Type: querypb.Type_INT32},
		{Name: "State", Type: querypb.Type_VARCHAR},
		{Name: "Info", Type: querypb.Type_VARCHAR},
		{Name: "Rows_sent", Type: querypb.Type_INT64},
		{Name: "Rows_examined", Type: querypb.Type_INT64},
	}

	var sessionInfos []SessionInfo
	privilegePlug := spanner.plugins.PlugPrivilege()
	if privilegePlug.IsSuperPriv(session.User()) {
		sessionInfos = sessions.Snapshot()
	} else {
		sessionInfos = sessions.SnapshotUser(session.User())
	}

	for _, info := range sessionInfos {
		row := []sqltypes.Value{
			sqltypes.MakeTrusted(querypb.Type_INT64, []byte(fmt.Sprintf("%v", info.ID))),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(info.User)),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(info.Host)),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(info.DB)),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(info.Command)),
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte(fmt.Sprintf("%v", info.Time))),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(info.State)),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(info.Info)),
			sqltypes.MakeTrusted(querypb.Type_INT64, []byte(fmt.Sprintf("%v", 0))),
			sqltypes.MakeTrusted(querypb.Type_INT64, []byte(fmt.Sprintf("%v", 0))),
		}
		qr.Rows = append(qr.Rows, row)
	}
	return qr, nil
}

// handleShowStatus used to handle the query "SHOW STATUS".
func (spanner *Spanner) handleShowStatus(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	var varname string
	log := spanner.log
	scatter := spanner.scatter

	privilegePlug := spanner.plugins.PlugPrivilege()
	if !privilegePlug.IsSuperPriv(session.User()) {
		return nil, sqldb.NewSQLErrorf(sqldb.ER_SPECIFIC_ACCESS_DENIED_ERROR, "Access denied; lacking super privilege for the operation")
	}

	qr := &sqltypes.Result{}
	qr.Fields = []*querypb.Field{
		{Name: "Variable_name", Type: querypb.Type_VARCHAR},
		{Name: "Value", Type: querypb.Type_VARCHAR},
	}

	// 1. radon_rate row.
	varname = "radon_rate"
	rate := scatter.QueryRates()
	qr.Rows = append(qr.Rows, []sqltypes.Value{
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(varname)),
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(rate.String())),
	})

	// 2. radon_config row.
	var confJSON []byte
	varname = "radon_config"
	type confShow struct {
		MaxConnections int      `json:"max-connections"`
		MaxResultSize  int      `json:"max-result-size"`
		DDLTimeout     int      `json:"ddl-timeout"`
		QueryTimeout   int      `json:"query-timeout"`
		TwopcEnable    bool     `json:"twopc-enable"`
		AllowIP        []string `json:"allow-ip"`
		AuditMode      string   `json:"audit-log-mode"`
		ReadOnly       bool     `json:"readonly"`
		Throttle       int      `json:"throttle"`
	}
	conf := confShow{
		MaxConnections: spanner.conf.Proxy.MaxConnections,
		MaxResultSize:  spanner.conf.Proxy.MaxResultSize,
		DDLTimeout:     spanner.conf.Proxy.DDLTimeout,
		QueryTimeout:   spanner.conf.Proxy.QueryTimeout,
		TwopcEnable:    spanner.conf.Proxy.TwopcEnable,
		AllowIP:        spanner.conf.Proxy.IPS,
		AuditMode:      spanner.conf.Audit.Mode,
		ReadOnly:       spanner.readonly.Get(),
		Throttle:       spanner.throttle.Limits(),
	}
	if b, err := json.Marshal(conf); err != nil {
		confJSON = []byte(err.Error())
	} else {
		confJSON = b
	}
	qr.Rows = append(qr.Rows, []sqltypes.Value{
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(varname)),
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(confJSON)),
	})

	// 3. radon_counter row.
	varname = "radon_transaction"
	txnCounter := scatter.TxnCounters()
	qr.Rows = append(qr.Rows, []sqltypes.Value{
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(varname)),
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(txnCounter.String())),
	})

	// 4. radon_backend_pool row.
	var poolJSON []byte
	varname = "radon_backendpool"
	type poolShow struct {
		Pools []string
	}
	be := poolShow{}
	poolz := scatter.PoolClone()
	for _, v := range poolz {
		be.Pools = append(be.Pools, v.JSON())
	}

	sort.Strings(be.Pools)
	if b, err := json.MarshalIndent(be, "", "\t\t\t"); err != nil {
		poolJSON = []byte(err.Error())
	} else {
		poolJSON = b
	}
	qr.Rows = append(qr.Rows, []sqltypes.Value{
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(varname)),
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(poolJSON)),
	})

	// 5. backends row.
	var backendsJSON []byte
	varname = "radon_backend"
	type backendShow struct {
		Backends []string
	}
	bs := backendShow{}

	backShowFunc := func(backend string, qr *sqltypes.Result) {
		tables := "0"
		datasize := "0MB"

		if len(qr.Rows) > 0 {
			tables = string(qr.Rows[0][0].Raw())
			if string(qr.Rows[0][1].Raw()) != "" {
				datasize = string(qr.Rows[0][1].Raw()) + "MB"
			}
		}
		buff := bytes.NewBuffer(make([]byte, 0, 256))
		fmt.Fprintf(buff, `{"name": "%s","tables": "%s", "datasize":"%s"}`, backend, tables, datasize)
		bs.Backends = append(bs.Backends, buff.String())
	}

	sql := "select count(0), round((sum(data_length) + sum(index_length)) / 1024/ 1024, 0)  from information_schema.TABLES  where table_schema not in ('sys', 'information_schema', 'mysql', 'performance_schema')"
	backends := spanner.scatter.AllBackends()
	for _, backend := range backends {
		qr, err := spanner.ExecuteOnThisBackend(backend, sql)
		if err != nil {
			log.Error("proxy.show.execute.on.this.backend[%x].error:%+v", backend, err)
		} else {
			backShowFunc(backend, qr)
		}
	}

	sort.Strings(bs.Backends)
	if b, err := json.MarshalIndent(bs, "", "\t\t\t"); err != nil {
		backendsJSON = []byte(err.Error())
	} else {
		backendsJSON = b
	}
	qr.Rows = append(qr.Rows, []sqltypes.Value{
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(varname)),
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(backendsJSON)),
	})

	return qr, nil
}

// handleShowQueryz used to handle the query "SHOW QUERYZ".
func (spanner *Spanner) handleShowQueryz(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	privilegePlug := spanner.plugins.PlugPrivilege()
	if !privilegePlug.IsSuperPriv(session.User()) {
		return nil, sqldb.NewSQLErrorf(sqldb.ER_SPECIFIC_ACCESS_DENIED_ERROR, "Access denied; lacking super privilege for the operation")
	}

	qr := &sqltypes.Result{}
	qr.Fields = []*querypb.Field{
		{Name: "ConnID", Type: querypb.Type_INT64},
		{Name: "Host", Type: querypb.Type_VARCHAR},
		{Name: "Start", Type: querypb.Type_VARCHAR},
		{Name: "Duration", Type: querypb.Type_INT32},
		{Name: "Query", Type: querypb.Type_VARCHAR},
	}
	rows := spanner.scatter.Queryz().GetQueryzRows()
	for _, row := range rows {
		row := []sqltypes.Value{
			sqltypes.MakeTrusted(querypb.Type_INT64, []byte(fmt.Sprintf("%v", uint64(row.ConnID)))),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(row.Address)),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(row.Start.Format("20060102150405.000"))),
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte(fmt.Sprintf("%v", row.Duration))),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(row.Query)),
		}
		qr.Rows = append(qr.Rows, row)
	}
	return qr, nil
}

// handleShowTxnz used to handle the query "SHOW TXNZ".
func (spanner *Spanner) handleShowTxnz(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	privilegePlug := spanner.plugins.PlugPrivilege()
	if !privilegePlug.IsSuperPriv(session.User()) {
		return nil, sqldb.NewSQLErrorf(sqldb.ER_SPECIFIC_ACCESS_DENIED_ERROR, "Access denied; lacking super privilege for the operation")
	}

	qr := &sqltypes.Result{}
	qr.Fields = []*querypb.Field{
		{Name: "TxnID", Type: querypb.Type_INT64},
		{Name: "Start", Type: querypb.Type_VARCHAR},
		{Name: "Duration", Type: querypb.Type_INT32},
		{Name: "XaState", Type: querypb.Type_VARCHAR},
		{Name: "TxnState", Type: querypb.Type_VARCHAR},
	}

	rows := spanner.scatter.Txnz().GetTxnzRows()
	for _, row := range rows {
		row := []sqltypes.Value{
			sqltypes.MakeTrusted(querypb.Type_INT64, []byte(fmt.Sprintf("%v", uint64(row.TxnID)))),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(row.Start.Format("20060102150405.000"))),
			sqltypes.MakeTrusted(querypb.Type_INT32, []byte(fmt.Sprintf("%v", row.Duration))),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(row.XaState)),
			sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(row.State)),
		}
		qr.Rows = append(qr.Rows, row)
	}
	return qr, nil
}

func (spanner *Spanner) handleShowVersions(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	qr := &sqltypes.Result{}
	qr.Fields = []*querypb.Field{
		{Name: "Versions", Type: querypb.Type_VARCHAR},
	}

	build := build.GetInfo()
	row := []sqltypes.Value{
		sqltypes.MakeTrusted(querypb.Type_VARCHAR, []byte(fmt.Sprintf("radon:%+v", build))),
	}
	qr.Rows = append(qr.Rows, row)
	return qr, nil
}

func (spanner *Spanner) handleJDBCShows(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	return spanner.ExecuteSingle(query)
}
