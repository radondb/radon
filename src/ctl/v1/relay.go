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
	"time"

	"proxy"

	"github.com/ant0ine/go-json-rest/rest"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// RelayStatusHandler impl.
func RelayStatusHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		relayStatusHandler(log, proxy, w, r)
	}
	return f
}

func relayStatusHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	type status struct {
		Status          bool   `json:"status"`
		MaxWorkers      int32  `json:"max-workers"`
		ParallelWorkers int32  `json:"parallel-workers"`
		SecondBehinds   int64  `json:"second-behinds"`
		ParallelType    int32  `json:"parallel-type"`
		RelayBinlog     string `json:"relay-binlog"`
		RelayGTID       int64  `json:"relay-gtid"`
		RestartGTID     int64  `json:"restart-gtid"`
		Rates           string `json:"rates"`
	}

	spanner := proxy.Spanner()
	bin := proxy.Binlog()
	backupRelay := spanner.BackupRelay()
	rsp := &status{
		Status:          backupRelay.RelayStatus(),
		MaxWorkers:      backupRelay.MaxWorkers(),
		ParallelWorkers: backupRelay.ParallelWorkers(),
		SecondBehinds:   (bin.LastGTID() - backupRelay.RelayGTID()) / int64(time.Second),
		ParallelType:    backupRelay.ParallelType(),
		RelayBinlog:     backupRelay.RelayBinlog(),
		RelayGTID:       backupRelay.RelayGTID(),
		RestartGTID:     backupRelay.RestartGTID(),
		Rates:           backupRelay.RelayRates(),
	}
	w.WriteJson(rsp)
}

// RelayInfosHandler impl.
func RelayInfosHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		relayInfosHandler(log, proxy, w, r)
	}
	return f
}

func relayInfosHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	bin := proxy.Binlog()
	rsp := bin.RelayInfos()
	w.WriteJson(rsp)
}

// RelayStartHandler impl.
func RelayStartHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		log.Warning("api.v1.relay.start[from:%v]", r.RemoteAddr)
		backupRelay := proxy.Spanner().BackupRelay()
		backupRelay.StartRelayWorker()
	}
	return f
}

// RelayStopHandler impl.
func RelayStopHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		log.Warning("api.v1.relay.stop[from:%v]", r.RemoteAddr)
		backupRelay := proxy.Spanner().BackupRelay()
		backupRelay.StopRelayWorker()
	}
	return f
}

type parallelTypeParams struct {
	Type int32 `json:"type"`
}

// RelayParallelTypeHandler impl.
func RelayParallelTypeHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		relayParallelTypeHandler(log, proxy, w, r)
	}
	return f
}

func relayParallelTypeHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	p := parallelTypeParams{}
	err := r.DecodeJsonPayload(&p)
	if err != nil {
		log.Error("api.v1.relay.parallel.type.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Warning("api.v1.relay.parallel.type.body:%+v", p)
	backupRelay := proxy.Spanner().BackupRelay()
	backupRelay.SetParallelType(p.Type)
}

// RelayResetHandler impl.
func RelayResetHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		relayResetHandler(log, proxy, w, r)
	}
	return f
}

type resetParams struct {
	GTID int64 `json:"gtid"`
}

func relayResetHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	backupRelay := proxy.Spanner().BackupRelay()
	if backupRelay.RelayStatus() {
		msg := "api.v1.relay.is.running.cant.reset.gitd.please.stop.first:relay stop"
		log.Error(msg)
		rest.Error(w, msg, http.StatusInternalServerError)
		return
	}

	p := resetParams{}
	err := r.DecodeJsonPayload(&p)
	if err != nil {
		log.Error("api.v1.relay.reset.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if p.GTID < 1514254947594569594 {
		msg := fmt.Sprintf("api.v1.relay.gtid[%v].less.than[1514254947594569594].should.be.UTC().UnixNano()", p.GTID)
		log.Error(msg)
		rest.Error(w, msg, http.StatusInternalServerError)
	}

	log.Warning("api.v1.relay.reset[from:%v].gtid:%+v", r.RemoteAddr, p)
	backupRelay.ResetRelayWorker(p.GTID)
}

// RelayWorkersHandler impl.
func RelayWorkersHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		relayWorkersHandler(log, proxy, w, r)
	}
	return f
}

type workersParams struct {
	Workers int `json:"workers"`
}

func relayWorkersHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	p := workersParams{}
	err := r.DecodeJsonPayload(&p)
	if err != nil {
		log.Error("api.v1.relay.workers.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if p.Workers < 1 || p.Workers > 1024 {
		msg := fmt.Sprintf("api.v1.relay.workers[%v].not.in[1, 1024]", p.Workers)
		log.Error(msg)
		rest.Error(w, msg, http.StatusInternalServerError)
	}

	backupRelay := proxy.Spanner().BackupRelay()
	log.Warning("api.v1.relay.set.max.worker.from[%v].to[%v]", backupRelay.MaxWorkers(), p.Workers)
	backupRelay.SetMaxWorkers(int32(p.Workers))
}
