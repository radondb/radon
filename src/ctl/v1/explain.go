/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package v1

import (
	"net/http"

	"optimizer"
	"proxy"

	"github.com/ant0ine/go-json-rest/rest"
	"github.com/xelabs/go-mysqlstack/xlog"

	"github.com/xelabs/go-mysqlstack/sqlparser"
)

type explainParams struct {
	Query string `json:"query"`
}

// ExplainHandler impl.
func ExplainHandler(log *xlog.Log, proxy *proxy.Proxy) rest.HandlerFunc {
	f := func(w rest.ResponseWriter, r *rest.Request) {
		explainHandler(log, proxy, w, r)
	}
	return f
}

func explainHandler(log *xlog.Log, proxy *proxy.Proxy, w rest.ResponseWriter, r *rest.Request) {
	type resp struct {
		Msg string
	}
	p := explainParams{}
	err := r.DecodeJsonPayload(&p)
	if err != nil {
		log.Error("api.v1.explain.error:%+v", err)
		rest.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	rsp := &resp{}
	router := proxy.Router()
	query := p.Query
	node, err := sqlparser.Parse(query)
	if err != nil {
		log.Error("ctl.v1.explain[%s].parser.error:%+v", query, err)
		rsp.Msg = err.Error()
		w.WriteJson(rsp)
		return
	}
	simOptimizer := optimizer.NewSimpleOptimizer(log, "", query, node, router)
	planTree, err := simOptimizer.BuildPlanTree()
	if err != nil {
		log.Error("ctl.v1.explain[%s].build.plan.error:%+v", query, err)
		rsp.Msg = err.Error()
		w.WriteJson(rsp)
		return
	}
	if len(planTree.Plans()) > 0 {
		rsp.Msg = planTree.Plans()[0].JSON()
		w.WriteJson(rsp)
	}
}
