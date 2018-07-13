/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package v1

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"proxy"

	"github.com/ant0ine/go-json-rest/rest"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// ShardzHandler impl.
func ShardzHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		shardzHandler(log, proxy, w, r)
	}
	return f
}

func shardzHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	router := proxy.Router()
	rulez := router.Rules()
	w.WriteJson(rulez)
}

// ShardBalanceAdviceHandler impl.
func ShardBalanceAdviceHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		shardBalanceAdviceHandler(log, proxy, w, r)
	}
	return f
}

// shardBalanceAdviceHandler used to get the advice who will be transfered.
// The Find algothm as follows:
// 1. first to sync all 'from.databases' to 'to.databases'
//
// 2. find the max datasize backend and min datasize backend.
//    1.1 max-datasize - min.datasize > 1GB
//    1.2 transfer path is: max --> min
//
// 3. find the best table(advice-table) to tansfer:
//    2.1 max.datasize - advice-table-size > min.datasize + advice-table-size
//
// Returns:
// 1. Status:200, Body:null
// 2. Status:503
// 3. Status:200, Body:JSON
func shardBalanceAdviceHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	scatter := proxy.Scatter()
	spanner := proxy.Spanner()
	backends := scatter.Backends()

	type backendSize struct {
		name    string
		address string
		size    float64
		user    string
		passwd  string
	}

	// 1.Find the max and min backend.
	var max, min backendSize
	for _, backend := range backends {
		query := "select round((sum(data_length) + sum(index_length)) / 1024/ 1024, 0)  as SizeInMB from information_schema.tables"
		qr, err := spanner.ExecuteOnThisBackend(backend, query)
		if err != nil {
			log.Error("api.v1.balance.advice.backend[%s].error:%+v", backend, err)
			rest.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if len(qr.Rows) > 0 {
			valStr := string(qr.Rows[0][0].Raw())
			datasize, err := strconv.ParseFloat(valStr, 64)
			if err != nil {
				log.Error("api.v1.balance.advice.parse.value[%s].error:%+v", valStr, err)
				rest.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			if datasize > max.size {
				max.name = backend
				max.size = datasize
			}

			if min.size == 0 {
				min.name = backend
				min.size = datasize
			}
			if datasize < min.size {
				min.name = backend
				min.size = datasize
			}
		}
	}
	log.Warning("api.v1.balance.advice.max:[%+v], min:[%+v]", max, min)

	// 2. Try to sync all databases from max.databases to min.databases.
	query := "show databases"
	qr, err := spanner.ExecuteOnThisBackend(max.name, query)
	if err != nil {
		log.Error("api.v1.balance.advice.show.databases.from[%+v].error:%+v", max, err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var sysDatabases = map[string]bool{
		"sys":                true,
		"mysql":              true,
		"information_schema": true,
		"performance_schema": true,
	}
	for _, row := range qr.Rows {
		db := string(row[0].Raw())
		if _, isSystem := sysDatabases[strings.ToLower(db)]; !isSystem {
			query1 := fmt.Sprintf("create database if not exists `%s`", db)
			if _, err := spanner.ExecuteOnThisBackend(min.name, query1); err != nil {
				log.Error("api.v1.balance.advice.create.database[%s].on[%+v].error:%+v", query1, min, err)
				rest.Error(w, err.Error(), http.StatusInternalServerError)
			}
			log.Warning("api.v1.balance.advice.create.database[%s].on[%+v].done", query1, min)
		}
	}
	log.Warning("api.v1.balance.advice.sync.database.done")

	// The differ must big than 256MB.
	delta := float64(256)
	differ := (max.size - min.size)
	if differ < delta {
		log.Warning("api.v1.balance.advice.return.nil.since.differ[%+vMB].less.than.%vMB", differ, delta)
		w.WriteJson(nil)
		return
	}

	backendConfs := scatter.BackendConfigsClone()
	for _, bconf := range backendConfs {
		if bconf.Name == max.name {
			max.address = bconf.Address
			max.user = bconf.User
			max.passwd = bconf.Password
		} else if bconf.Name == min.name {
			min.address = bconf.Address
			min.user = bconf.User
			min.passwd = bconf.Password
		}
	}

	// 3. Find the best table.
	query = "SELECT table_schema, table_name, ROUND((SUM(data_length+index_length)) / 1024/ 1024, 0) AS sizeMB FROM information_schema.TABLES GROUP BY table_name HAVING SUM(data_length + index_length)>10485760 ORDER BY (data_length + index_length) DESC"
	qr, err = spanner.ExecuteOnThisBackend(max.name, query)
	if err != nil {
		log.Error("api.v1.balance.advice.get.max[%+v].tables.error:%+v", max, err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var tableSize float64
	var database, table string
	for _, row := range qr.Rows {
		db := string(row[0].Raw())
		tbl := string(row[1].Raw())
		valStr := string(row[2].Raw())
		tblSize, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			log.Error("api.v1.balance.advice.get.tables.parse.value[%s].error:%+v", valStr, err)
			rest.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Make sure the table is small enough.
		if (min.size + tblSize) < (max.size - tblSize) {
			// Find the advice table.
			database = db
			table = tbl
			tableSize = tblSize
			break
		}
	}

	// No best.
	if database == "" || table == "" {
		log.Warning("api.v1.balance.advice.return.nil.since.cant.find.the.best.table")
		w.WriteJson(nil)
		return
	}

	type balanceAdvice struct {
		From         string  `json:"from-address"`
		FromDataSize float64 `json:"from-datasize"`
		FromUser     string  `json:"from-user"`
		FromPasswd   string  `json:"from-password"`
		To           string  `json:"to-address"`
		ToDataSize   float64 `json:"to-datasize"`
		ToUser       string  `json:"to-user"`
		ToPasswd     string  `json:"to-password"`
		Database     string  `json:"database"`
		Table        string  `json:"table"`
		TableSize    float64 `json:"tablesize"`
	}

	advice := balanceAdvice{
		From:         max.address,
		FromDataSize: max.size,
		FromUser:     max.user,
		FromPasswd:   max.passwd,
		To:           min.address,
		ToDataSize:   min.size,
		ToUser:       min.user,
		ToPasswd:     min.passwd,
		Database:     database,
		Table:        table,
		TableSize:    tableSize,
	}
	log.Warning("api.v1.balance.advice.return:%+v", advice)
	w.WriteJson(advice)
}

type ruleParams struct {
	Database    string `json:"database"`
	Table       string `json:"table"`
	FromAddress string `json:"from-address"`
	ToAddress   string `json:"to-address"`
}

// ShardRuleShiftHandler used to shift a partition rule to another backend.
func ShardRuleShiftHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		shardRuleShiftHandler(log, proxy, w, r)
	}
	return f
}

var sysDBs = []string{"information_schema", "mysql", "performance_schema", "sys"}

func shardRuleShiftHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	router := proxy.Router()
	scatter := proxy.Scatter()
	p := ruleParams{}
	err := r.DecodeJsonPayload(&p)
	if err != nil {
		log.Error("api.v1.radon.shard.rule.parse.json.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Warning("api.v1.radon.shard.rule[from:%v].request:%+v", r.RemoteAddr, p)

	if p.Database == "" || p.Table == "" {
		rest.Error(w, "api.v1.shard.rule.request.database.or.table.is.null", http.StatusInternalServerError)
		return
	}

	for _, sysDB := range sysDBs {
		if sysDB == strings.ToLower(p.Database) {
			log.Error("api.v1.shard.rule.database[%s].is.system", p.Database)
			rest.Error(w, "api.v1.shard.rule.database.can't.be.system.database", http.StatusInternalServerError)
			return
		}
	}

	var fromBackend, toBackend string
	backends := scatter.BackendConfigsClone()
	for _, backend := range backends {
		if backend.Address == p.FromAddress {
			fromBackend = backend.Name
		} else if backend.Address == p.ToAddress {
			toBackend = backend.Name
		}
	}

	if fromBackend == "" || toBackend == "" {
		log.Error("api.v1.shard.rule.fromBackend[%s].or.toBackend[%s].is.NULL", fromBackend, toBackend)
		rest.Error(w, "api.v1.shard.rule.backend.NULL", http.StatusInternalServerError)
		return
	}

	if err := router.PartitionRuleShift(fromBackend, toBackend, p.Database, p.Table); err != nil {
		log.Error("api.v1.shard.rule.PartitionRuleShift.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// ShardReLoadHandler impl.
func ShardReLoadHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		shardReLoadHandler(log, proxy, w, r)
	}
	return f
}

func shardReLoadHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	router := proxy.Router()
	log.Warning("api.shard.reload.prepare.from[%v]...", r.RemoteAddr)
	if err := router.ReLoad(); err != nil {
		log.Error("api.v1.shard.reload.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Warning("api.shard.reload.done...")
}
