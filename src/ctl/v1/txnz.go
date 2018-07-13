/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package v1

import (
	"strconv"
	"time"

	"proxy"

	"github.com/ant0ine/go-json-rest/rest"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// TxnzHandler impl.
func TxnzHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		txnzHandler(log, proxy, w, r)
	}
	return f
}

func txnzHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	type txn struct {
		TxnID    uint64        `json:"txnid"`
		Start    time.Time     `json:"start"`
		Duration time.Duration `json:"duration"`
		State    string        `json:"state"`
		XaState  string        `json:"xa-state"`
		Color    string        `json:"color"`
	}

	limit := 100
	if v, err := strconv.Atoi(r.PathParam("limit")); err == nil {
		limit = v
	}

	var rsp []txn
	scatter := proxy.Scatter()
	rows := scatter.Txnz().GetTxnzRows()
	for i, row := range rows {
		if i >= limit {
			break
		}
		r := txn{
			TxnID:    row.TxnID,
			Start:    row.Start,
			Duration: row.Duration,
			State:    row.State,
			XaState:  row.XaState,
			Color:    row.Color,
		}
		rsp = append(rsp, r)
	}
	w.WriteJson(rsp)
}
