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

	"config"

	"github.com/xelabs/go-mysqlstack/xlog"
)

// IP tuple.
type IP struct {
	ip string
}

// IPTable tuple.
type IPTable struct {
	mu      sync.RWMutex
	log     *xlog.Log
	conf    *config.ProxyConfig
	iptable map[string]*IP
}

// NewIPTable creates a new IPTable.
func NewIPTable(log *xlog.Log, conf *config.ProxyConfig) *IPTable {
	ipt := &IPTable{
		log:     log,
		conf:    conf,
		iptable: make(map[string]*IP),
	}

	if conf.IPS != nil {
		for _, ip := range conf.IPS {
			IP := &IP{ip: ip}
			ipt.iptable[ip] = IP
		}
	}
	return ipt
}

// Add used to add a ip to table.
func (ipt *IPTable) Add(ip string) {
	ipt.log.Warning("proxy.iptable.add:%s", ip)
	ipt.mu.Lock()
	defer ipt.mu.Unlock()
	IP := &IP{ip: ip}
	ipt.iptable[ip] = IP
}

// Remove used to remove a ip from table.
func (ipt *IPTable) Remove(ip string) {
	ipt.log.Warning("proxy.iptable.remove:%s", ip)
	ipt.mu.Lock()
	defer ipt.mu.Unlock()
	delete(ipt.iptable, ip)
}

// Refresh used to refresh the table.
func (ipt *IPTable) Refresh() {
	ipt.log.Warning("proxy.iptable.refresh:%+v", ipt.conf.IPS)
	ipt.mu.Lock()
	defer ipt.mu.Unlock()

	ipt.iptable = make(map[string]*IP)
	if ipt.conf.IPS != nil {
		for _, ip := range ipt.conf.IPS {
			IP := &IP{ip: ip}
			ipt.iptable[ip] = IP
		}
	}
}

// Check used to check a whether the ip is in ip table or not.
func (ipt *IPTable) Check(address string) bool {
	ipt.mu.Lock()
	defer ipt.mu.Unlock()

	if len(ipt.iptable) == 0 {
		return true
	}
	if _, ok := ipt.iptable[address]; !ok {
		return false
	}
	return true
}
