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
	"sort"
	"strings"

	"build"

	"github.com/xelabs/go-mysqlstack/common"
	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/sqldb"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/sqltypes"
)

// handleShowDatabases used to handle the 'SHOW DATABASES' command.
func (spanner *Spanner) handleShowDatabases(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	return spanner.ExecuteSingle(query)
}

// handleShowEngines used to handle the 'SHOW ENGINES' command.
func (spanner *Spanner) handleShowEngines(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	return spanner.ExecuteSingle(query)
}

// handleShowCreateDatabase used to handle the 'SHOW CREATE DATABASE' command.
func (spanner *Spanner) handleShowCreateDatabase(session *driver.Session, query string, node sqlparser.Statement) (*sqltypes.Result, error) {
	return spanner.ExecuteSingle(query)
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
		return nil, sqldb.NewSQLError(sqldb.ER_NO_DB_ERROR, "")
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
		return nil, sqldb.NewSQLError(sqldb.ER_NO_DB_ERROR, "")
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

	// If shardType is GLOBAL, send raw query; if shardType is HASH, rewrite the query.
	if shardKey == "" {
		qr, err = spanner.ExecuteSingle(query)
		if err != nil {
			return nil, err
		}
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
		return nil, sqldb.NewSQLError(sqldb.ER_NO_DB_ERROR, "")
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
	sessionInfos := sessions.Snapshot()
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
	backends := spanner.scatter.Backends()
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
