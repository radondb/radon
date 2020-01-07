/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package ctl

import (
	v1 "ctl/v1"

	"github.com/ant0ine/go-json-rest/rest"
)

// NewRouter creates the new router.
func (admin *Admin) NewRouter() (rest.App, error) {
	log := admin.log
	proxy := admin.proxy

	return rest.MakeRouter(
		// radon
		rest.Post("/v1/radon/explain", v1.ExplainHandler(log, proxy)),
		rest.Put("/v1/radon/config", v1.RadonConfigHandler(log, proxy)),
		rest.Get("/v1/radon/ping", v1.PingHandler(log, proxy)),
		rest.Put("/v1/radon/readonly", v1.ReadonlyHandler(log, proxy)),
		rest.Put("/v1/radon/twopc", v1.TwopcHandler(log, proxy)),
		rest.Put("/v1/radon/throttle", v1.ThrottleHandler(log, proxy)),
		rest.Post("/v1/radon/backend", v1.AddBackendHandler(log, proxy)),
		rest.Delete("/v1/radon/backend/:name", v1.RemoveBackendHandler(log, proxy)),
		rest.Get("/v1/radon/restapiaddress", v1.RestAPIAddressHandler(log, proxy)),
		rest.Get("/v1/radon/status", v1.StatusHandler(log, proxy)),

		// user
		rest.Post("/v1/user/add", v1.CreateUserHandler(log, proxy)),
		rest.Post("/v1/user/update", v1.AlterUserHandler(log, proxy)),
		rest.Post("/v1/user/remove", v1.DropUserHandler(log, proxy)),
		rest.Get("/v1/user/userz", v1.UserzHandler(log, proxy)),

		// shard
		rest.Get("/v1/shard/shardz", v1.ShardzHandler(log, proxy)),
		rest.Get("/v1/shard/globals", v1.GlobalsHandler(log, proxy)),
		rest.Get("/v1/shard/balanceadvice", v1.ShardBalanceAdviceHandler(log, proxy)),
		rest.Post("/v1/shard/shift", v1.ShardRuleShiftHandler(log, proxy)),
		rest.Post("/v1/shard/reload", v1.ShardReLoadHandler(log, proxy)),
		rest.Post("/v1/shard/migrate", v1.ShardMigrateHandler(log, proxy)),

		// meta
		rest.Get("/v1/meta/versions", v1.VersionzHandler(log, proxy)),
		rest.Get("/v1/meta/versioncheck", v1.VersionCheckHandler(log, proxy)),
		rest.Get("/v1/meta/metas", v1.MetazHandler(log, proxy)),

		// peer
		rest.Get("/v1/peer/peerz", v1.PeerzHandler(log, proxy)),
		rest.Post("/v1/peer/add", v1.AddPeerHandler(log, proxy)),
		rest.Post("/v1/peer/remove", v1.RemovePeerHandler(log, proxy)),

		// debug
		rest.Get("/v1/debug/processlist", v1.ProcesslistHandler(log, proxy)),
		rest.Get("/v1/debug/queryz/:limit", v1.QueryzHandler(log, proxy)),
		rest.Get("/v1/debug/txnz/:limit", v1.TxnzHandler(log, proxy)),
		rest.Get("/v1/debug/configz", v1.ConfigzHandler(log, proxy)),
		rest.Get("/v1/debug/backendz", v1.BackendzHandler(log, proxy)),
		rest.Get("/v1/debug/schemaz", v1.SchemazHandler(log, proxy)),
	)
}
