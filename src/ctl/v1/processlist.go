/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package v1

import (
	"proxy"

	"github.com/ant0ine/go-json-rest/rest"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// ProcesslistHandler impl.
func ProcesslistHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		processlistHandler(log, proxy, w, r)
	}
	return f
}

func processlistHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	type processlist struct {
		ID      uint32 `json:"id"`
		User    string `json:"user"`
		Host    string `json:"host"`
		DB      string `json:"db"`
		Command string `json:"command"`
		Time    uint32 `json:"time"`
		State   string `json:"state"`
		Info    string `json:"info"`
	}

	var rsp []processlist
	sessions := proxy.Sessions()
	rows := sessions.Snapshot()
	for _, sr := range rows {
		r := processlist{
			ID:      sr.ID,
			User:    sr.User,
			Host:    sr.Host,
			DB:      sr.DB,
			Command: sr.Command,
			Time:    sr.Time,
			State:   sr.State,
			Info:    sr.Info,
		}
		rsp = append(rsp, r)
	}
	w.WriteJson(rsp)
}
