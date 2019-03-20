/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package plugins

import (
	"backend"
	"config"
	"router"

	"plugins/autoincrement"

	"github.com/xelabs/go-mysqlstack/xlog"
)

// Plugin --
type Plugin struct {
	log          *xlog.Log
	conf         *config.Config
	router       *router.Router
	scatter      *backend.Scatter
	autoincement *autoincrement.AutoIncrement
}

// NewPlugin -- creates new Plugin.
func NewPlugin(log *xlog.Log, conf *config.Config, router *router.Router, scatter *backend.Scatter) *Plugin {
	return &Plugin{
		log:     log,
		conf:    conf,
		router:  router,
		scatter: scatter,
	}
}

// Init -- used to regeister plug to plugins.
func (plugin *Plugin) Init() error {
	log := plugin.log
	router := plugin.router

	// Regeister AutoIncrement plug.
	autoincPlug := autoincrement.NewAutoIncrement(log, router)
	if err := autoincPlug.Init(); err != nil {
		return err
	}
	plugin.autoincement = autoincPlug
	return nil
}

// Close -- do nothing.
func (plugin *Plugin) Close() {
}

// PlugAutoIncrement -- return AutoIncrement plug.
func (plugin *Plugin) PlugAutoIncrement() *autoincrement.AutoIncrement {
	return plugin.autoincement
}
