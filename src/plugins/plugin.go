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
	"plugins/privilege"
	"plugins/shiftmanager"

	"github.com/xelabs/go-mysqlstack/xlog"
)

// Plugin --
type Plugin struct {
	log           *xlog.Log
	conf          *config.Config
	router        *router.Router
	scatter       *backend.Scatter
	autoincrement autoincrement.AutoIncrementHandler
	privilege     privilege.PrivilegeHandler
	shiftMgr      shiftmanager.ShiftMgrHandler
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

// Init -- used to register plug to plugins.
func (plugin *Plugin) Init() error {
	log := plugin.log
	router := plugin.router
	scatter := plugin.scatter
	config := plugin.conf

	// Register AutoIncrement plug.
	autoincPlug := autoincrement.NewAutoIncrement(log, router)
	if err := autoincPlug.Init(); err != nil {
		return err
	}
	plugin.autoincrement = autoincPlug

	// Register privilege plug.
	privilegePlug := privilege.NewPrivilege(log, config, scatter)
	if err := privilegePlug.Init(); err != nil {
		return err
	}
	plugin.privilege = privilegePlug

	// Register shiftmanager plug
	shiftMgr := shiftmanager.NewShiftManager(log)
	if err := shiftMgr.Init(); err != nil {
		return err
	}
	plugin.shiftMgr = shiftMgr

	return nil
}

// Close -- do nothing.
func (plugin *Plugin) Close() {
	plugin.autoincrement.Close()
	plugin.privilege.Close()
	plugin.shiftMgr.Close()
}

// PlugAutoIncrement -- return AutoIncrement plug.
func (plugin *Plugin) PlugAutoIncrement() autoincrement.AutoIncrementHandler {
	return plugin.autoincrement
}

// PlugPrivilege -- return Privilege plug.
func (plugin *Plugin) PlugPrivilege() privilege.PrivilegeHandler {
	return plugin.privilege
}

// PlugShiftMgr -- return ShiftMgr plug.
func (plugin *Plugin) PlugShiftMgr() shiftmanager.ShiftMgrHandler {
	return plugin.shiftMgr
}
