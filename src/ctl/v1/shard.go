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
	"strings"

	"plugins/shiftmanager"
	"proxy"

	"github.com/ant0ine/go-json-rest/rest"
	shiftlog "github.com/radondb/shift/xlog"
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
// The Find algorithm as follows:
//
// 1. find the max datasize backend and min datasize backend.
//    1.1 max-datasize - min.datasize > 1GB
//    1.2 transfer path is: max --> min
//
// 2. find the best table(advice-table) to tansfer:
//    2.1 max.datasize - advice-table-size > min.datasize + advice-table-size
//
// Returns:
// 1. Status:200, Body:null
// 2. Status:500
// 3. Status:200, Body:JSON
func shardBalanceAdviceHandler(log *xlog.Log, p *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	max := &proxy.BackendSize{}
	min := &proxy.BackendSize{}
	var database, table string
	var tableSize float64

	if err := proxy.ShardBalanceAdvice(log, p.Spanner(), p.Scatter(), p.Router(), max, min, &database, &table, &tableSize); err != nil {
		log.Error("api.v1.balance.advice.return.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
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
		From:         max.Address,
		FromDataSize: max.Size,
		FromUser:     max.User,
		FromPasswd:   max.Passwd,
		To:           min.Address,
		ToDataSize:   min.Size,
		ToUser:       min.User,
		ToPasswd:     min.Passwd,
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

// GlobalsHandler used to get the global tables.
func GlobalsHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		globalsHandler(log, proxy, w, r)
	}
	return f
}

func globalsHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	router := proxy.Router()

	type databases struct {
		Database string   `json:"database"`
		Tables   []string `json:"tables"`
	}

	type schemas struct {
		Schemas []databases `json:"schemas"`
	}

	var globals schemas
	for _, schema := range router.Schemas {
		var tables []string
		for _, tb := range schema.Tables {
			if tb.TableConfig.ShardType == "GLOBAL" {
				tables = append(tables, tb.Name)
			}
		}
		if len(tables) > 0 {
			db := databases{
				Database: schema.DB,
				Tables:   tables,
			}
			globals.Schemas = append(globals.Schemas, db)
		}
	}

	if len(globals.Schemas) == 0 {
		log.Warning("api.v1.globals.return.nil.since.cant.find.the.global.tables")
		w.WriteJson(nil)
		return
	}
	w.WriteJson(globals)
}

type migrateParams struct {
	From         string `json:"from"`
	FromUser     string `json:"from-user"`
	FromPassword string `json:"from-password"`
	FromDatabase string `json:"from-database"`
	FromTable    string `json:"from-table"`

	To         string `json:"to"`
	ToUser     string `json:"to-user"`
	ToPassword string `json:"to-password"`
	ToDatabase string `json:"to-database"`
	ToTable    string `json:"to-table"`

	RadonURL               string `json:"radonurl"`
	Rebalance              bool   `json:"rebalance"`
	Cleanup                bool   `json:"cleanup"`
	MySQLDump              string `json:"mysqldump"`
	Threads                int    `json:"threads"`
	Behinds                int    `json:"behinds"`
	Checksum               bool   `json:"checksum"`
	WaitTimeBeforeChecksum int    `json:"wait-time-before-checksum"`
}

// ShardMigrateHandler used to migrate data from one backend to another.
// Returns:
// 1. Status:200
// 2. Status:204
// 3. Status:403
// 4. Status:500
func ShardMigrateHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		shardMigrateHandler(proxy, w, r)
	}
	return f
}

func shardMigrateHandler(proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	scatter := proxy.Scatter()
	log := shiftlog.NewStdLog(shiftlog.Level(shiftlog.INFO))
	p := &migrateParams{
		RadonURL:               "http://" + proxy.Config().Proxy.PeerAddress,
		Rebalance:              false,
		Cleanup:                false,
		MySQLDump:              "mysqldump",
		Threads:                16,
		Behinds:                2048,
		Checksum:               true,
		WaitTimeBeforeChecksum: 10,
	}
	err := r.DecodeJsonPayload(&p)
	if err != nil {
		log.Error("api.v1.shard.migrate.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if proxy.Spanner().ReadOnly() {
		log.Error("api.v1.shard.migrate.error:The MySQL server is running with the --read-only option")
		rest.Error(w, "The MySQL server is running with the --read-only option", http.StatusForbidden)
		return
	}

	// check args.
	if len(p.FromUser) == 0 || len(p.FromDatabase) == 0 || len(p.FromTable) == 0 ||
		len(p.ToUser) == 0 || len(p.ToDatabase) == 0 || len(p.ToTable) == 0 {
		log.Error("api.v1.shard.migrate[%+v].error:some param is empty", p)
		rest.Error(w, "some args are empty", http.StatusNoContent)
		return
	}

	// Check the backend name.
	var fromBackend, toBackend string
	backends := scatter.BackendConfigsClone()
	for _, backend := range backends {
		if backend.Address == p.From {
			fromBackend = backend.Name
		} else if backend.Address == p.To {
			toBackend = backend.Name
		}
	}
	if fromBackend == "" || toBackend == "" {
		log.Error("api.v1.shard.migrate.fromBackend[%s].or.toBackend[%s].is.NULL", fromBackend, toBackend)
		rest.Error(w, "api.v1.shard.migrate.backend.NULL", http.StatusInternalServerError)
		return
	}

	cfg := &shiftmanager.ShiftInfo{
		From:                   p.From,
		FromUser:               p.FromUser,
		FromPassword:           p.FromPassword,
		FromDatabase:           p.FromDatabase,
		FromTable:              p.FromTable,
		To:                     p.To,
		ToUser:                 p.ToUser,
		ToPassword:             p.ToPassword,
		ToDatabase:             p.ToDatabase,
		ToTable:                p.ToTable,
		Rebalance:              p.Rebalance,
		Cleanup:                p.Cleanup,
		MysqlDump:              p.MySQLDump,
		Threads:                p.Threads,
		PosBehinds:             p.Behinds,
		RadonURL:               p.RadonURL,
		Checksum:               p.Checksum,
		WaitTimeBeforeChecksum: p.WaitTimeBeforeChecksum,
	}

	shiftMgr := proxy.Plugins().PlugShiftMgr()
	shift, _ := shiftMgr.NewShiftInstance(cfg, shiftmanager.ShiftTypeRebalance)

	key := fmt.Sprintf("`%s`.`%s`_%s", p.ToDatabase, p.ToTable, toBackend)
	err = shiftMgr.StartShiftInstance(key, shift, shiftmanager.ShiftTypeRebalance)
	if err != nil {
		log.Error("shift.start.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = shiftMgr.WaitInstanceFinish(key)
	if err != nil {
		log.Error("shift.wait.finish.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	log.Warning("api.v1.shard.migrate.done...")
}
