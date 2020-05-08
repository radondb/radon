/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"sync"

	"audit"
	"backend"
	"config"
	"plugins"
	"router"
	"syncer"
	"xbase"

	"github.com/xelabs/go-mysqlstack/driver"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// Proxy tuple.
type Proxy struct {
	mu            sync.RWMutex
	log           *xlog.Log
	conf          *config.Config
	confPath      string
	audit         *audit.Audit
	router        *router.Router
	scatter       *backend.Scatter
	syncer        *syncer.Syncer
	plugins       *plugins.Plugin
	iptable       *IPTable
	spanner       *Spanner
	sessions      *Sessions
	listener      *driver.Listener
	throttle      *xbase.Throttle
	serverVersion string
}

// NewProxy creates new proxy.
func NewProxy(log *xlog.Log, path string, serverVersion string, conf *config.Config) *Proxy {
	audit := audit.NewAudit(log, conf.Audit)
	router := router.NewRouter(log, conf.Proxy.MetaDir, conf.Router)
	scatter := backend.NewScatter(log, conf.Proxy.MetaDir)
	syncer := syncer.NewSyncer(log, conf.Proxy.MetaDir, conf.Proxy.PeerAddress, router, scatter)
	plugins := plugins.NewPlugin(log, conf, router, scatter)
	return &Proxy{
		log:           log,
		conf:          conf,
		confPath:      path,
		audit:         audit,
		router:        router,
		scatter:       scatter,
		syncer:        syncer,
		plugins:       plugins,
		sessions:      NewSessions(log),
		iptable:       NewIPTable(log, conf.Proxy),
		throttle:      xbase.NewThrottle(0),
		serverVersion: serverVersion,
	}
}

// Start used to start the proxy.
func (p *Proxy) Start() {
	log := p.log
	conf := p.conf
	audit := p.audit
	iptable := p.iptable
	syncer := p.syncer
	router := p.router
	scatter := p.scatter
	plugins := p.plugins
	sessions := p.sessions
	endpoint := conf.Proxy.Endpoint
	throttle := p.throttle
	serverVersion := p.serverVersion

	log.Info("proxy.config[%+v]...", conf.Proxy)
	log.Info("log.config[%+v]...", conf.Log)

	if err := audit.Init(); err != nil {
		log.Panic("proxy.audit.init.panic:%+v", err)
	}
	if err := syncer.Init(); err != nil {
		log.Panic("proxy.syncer.init.panic:%+v", err)
	}
	if err := router.LoadConfig(); err != nil {
		log.Panic("proxy.router.load.panic:%+v", err)
	}
	if err := scatter.LoadConfig(); err != nil {
		log.Panic("proxy.scatter.load.config.panic:%+v", err)
	}

	if err := scatter.Init(p.conf.Scatter); err != nil {
		log.Panic("proxy.scatter.init.panic:%+v", err)
	}

	if err := plugins.Init(); err != nil {
		log.Panic("proxy.plugins.init.panic:%+v", err)
	}

	spanner := NewSpanner(log, conf, iptable, router, scatter, sessions, audit, throttle, plugins, serverVersion)
	if err := spanner.Init(); err != nil {
		log.Panic("proxy.spanner.init.panic:%+v", err)
	}
	svr, err := driver.NewListener(log, endpoint, spanner)
	if err != nil {
		log.Panic("proxy.start.error[%+v]", err)
	}
	p.spanner = spanner
	p.listener = svr
	log.Info("proxy.start[%v]...", endpoint)
	go svr.Accept()
}

// Stop used to stop the proxy.
func (p *Proxy) Stop() {
	log := p.log

	log.Info("proxy.starting.shutdown...")
	p.sessions.Close()
	p.spanner.Close()
	p.listener.Close()
	p.scatter.Close()
	p.audit.Close()
	p.syncer.Close()
	p.plugins.Close()
	log.Info("proxy.shutdown.complete...")
}

// Config returns the config.
func (p *Proxy) Config() *config.Config {
	return p.conf
}

// Address returns the proxy endpoint.
func (p *Proxy) Address() string {
	return p.conf.Proxy.Endpoint
}

// IPTable returns the ip table.
func (p *Proxy) IPTable() *IPTable {
	return p.iptable
}

// Scatter returns the scatter.
func (p *Proxy) Scatter() *backend.Scatter {
	return p.scatter
}

// Router returns the router.
func (p *Proxy) Router() *router.Router {
	return p.router
}

// Syncer returns the syncer.
func (p *Proxy) Syncer() *syncer.Syncer {
	return p.syncer
}

// Plugins return the plugins.
func (p *Proxy) Plugins() *plugins.Plugin {
	return p.plugins
}

// Sessions returns the sessions.
func (p *Proxy) Sessions() *Sessions {
	return p.sessions
}

// Spanner returns the spanner.
func (p *Proxy) Spanner() *Spanner {
	return p.spanner
}

// SetMaxConnections used to set the max connections.
func (p *Proxy) SetMaxConnections(connections int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.log.Info("proxy.SetMaxConnections:[%d->%d]", p.conf.Proxy.MaxConnections, connections)
	p.conf.Proxy.MaxConnections = connections
}

// SetMaxResultSize used to set the max result size.
func (p *Proxy) SetMaxResultSize(size int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.log.Info("proxy.SetMaxResultSize:[%d->%d]", p.conf.Proxy.MaxResultSize, size)
	p.conf.Proxy.MaxResultSize = size
}

// SetMaxJoinRows used to set the max result size.
func (p *Proxy) SetMaxJoinRows(size int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.log.Info("proxy.SetMaxJoinRows:[%d->%d]", p.conf.Proxy.MaxJoinRows, size)
	p.conf.Proxy.MaxJoinRows = size
}

// SetDDLTimeout used to set the ddl timeout.
func (p *Proxy) SetDDLTimeout(timeout int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.log.Info("proxy.SetDDLTimeout:[%d->%d]", p.conf.Proxy.DDLTimeout, timeout)
	p.conf.Proxy.DDLTimeout = timeout
}

// SetQueryTimeout used to set query timeout.
func (p *Proxy) SetQueryTimeout(timeout int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.log.Info("proxy.SetQueryTimeout:[%d->%d]", p.conf.Proxy.QueryTimeout, timeout)
	p.conf.Proxy.QueryTimeout = timeout
}

// SetLongQueryTime Set long Query Time used to set long query time.
func (p *Proxy) SetLongQueryTime(longQueryTime int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.log.Info("proxy.SetQueryTimeout:[%d->%d]", p.conf.Proxy.LongQueryTime, longQueryTime)
	p.conf.Proxy.LongQueryTime = longQueryTime
}

// SetTwoPC used to set twopc to enable or disable.
func (p *Proxy) SetTwoPC(enable bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.log.Info("proxy.SetTwoPC:[%v->%v]", p.conf.Proxy.TwopcEnable, enable)
	p.conf.Proxy.TwopcEnable = enable
}

// SetAutocommitFalseIsTxn used to set autocommitFalseIsTxn to true or false.
func (p *Proxy) SetAutocommitFalseIsTxn(enable bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.log.Info("proxy.SetAutocommitFalseIsTxn:[%v->%v]", p.conf.Proxy.AutocommitFalseIsTxn, enable)
	p.conf.Proxy.AutocommitFalseIsTxn = enable
}

// SetAllowIP used to set allow ips.
func (p *Proxy) SetAllowIP(ips []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.log.Info("proxy.SetAllowIP:[%v->%v]", p.conf.Proxy.IPS, ips)
	p.conf.Proxy.IPS = ips
}

// SetAuditMode used to set the mode of audit.
func (p *Proxy) SetAuditMode(mode string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.log.Info("proxy.SetAuditMode:[%s->%s]", p.conf.Audit.Mode, mode)
	p.conf.Audit.Mode = mode
}

// SetReadOnly used to enable/disable readonly.
func (p *Proxy) SetReadOnly(val bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.log.Info("proxy.SetReadOnly:[%v->%v]", p.spanner.ReadOnly(), val)
	p.spanner.SetReadOnly(val)
}

// SetLoadBalance used to set loadbalance.
func (p *Proxy) SetLoadBalance(val int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.log.Info("proxy.SetLoadBalance:[%v->%v]", p.conf.Proxy.LoadBalance, val)
	p.conf.Proxy.LoadBalance = val
}

// PeerAddress returns the peer address.
func (p *Proxy) PeerAddress() string {
	return p.conf.Proxy.PeerAddress
}

// FlushConfig used to flush the config to disk.
func (p *Proxy) FlushConfig() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.log.Info("proxy.flush.config.to.file:%v, config:%+v", p.confPath, p.conf.Proxy)
	if err := config.WriteConfig(p.confPath, p.conf); err != nil {
		p.log.Error("proxy.flush.config.to.file[%v].error:%v", p.confPath, err)
		return err
	}
	return nil
}

// SetThrottle used to set the throttle.
func (p *Proxy) SetThrottle(val int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.log.Info("proxy.SetThrottle:[%v->%v]", p.throttle.Limits(), val)
	p.throttle.Set(val)
}

// SetStreamBufferSize used to set the streamBufferSize.
func (p *Proxy) SetStreamBufferSize(streamBufferSize int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.log.Info("proxy.SetStreamBufferSize:[%d->%d]", p.conf.Proxy.StreamBufferSize, streamBufferSize)
	p.conf.Proxy.StreamBufferSize = streamBufferSize
}

// SetBlocks used to set router blocks.
func (p *Proxy) SetBlocks(blocks int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.log.Info("proxy.SetBlocks:[%d->%d]", p.conf.Router.Blocks, blocks)
	p.conf.Router.Blocks = blocks
}
