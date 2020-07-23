/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"regexp"
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
	iptable map[string]interface{}
}

// NewIPTable creates a new IPTable.
func NewIPTable(log *xlog.Log, conf *config.ProxyConfig) *IPTable {
	ipt := &IPTable{
		log:     log,
		conf:    conf,
		iptable: make(map[string]interface{}),
	}

	if conf.IPS != nil {
		for _, ip := range conf.IPS {
			if err := addToIPTable(ipt, ip); err != nil {
				log.Error("add ip failed during new iptable: ip[%s], err[%+v]", ip, err)
			}
		}
	}
	return ipt
}

// isWildcardIP used to judge if ip is a regexp and return true, otherwise return false
func isWildcardIP(ip string) bool {
	return regexp.QuoteMeta(ip) != ip
}

// addToIPTable is used to add an ip to iptable
func addToIPTable(ipt *IPTable, ip string) error {
	if isWildcardIP(ip) {
		if ip == "*" {
			ip = "." + ip
		}
		reg, err := regexp.Compile(ip)
		if err != nil {
			return err
		}
		ipt.iptable[ip] = reg
	} else {
		IP := &IP{ip: ip}
		ipt.iptable[ip] = IP
	}
	return nil
}

// Add used to add an ip to iptable.
func (ipt *IPTable) Add(ip string) error {
	ipt.log.Warning("proxy.iptable.add:%s", ip)
	ipt.mu.Lock()
	defer ipt.mu.Unlock()
	return addToIPTable(ipt, ip)
}

// Remove used to remove a ip from table.
func (ipt *IPTable) Remove(ip string) {
	ipt.log.Warning("proxy.iptable.remove:%s", ip)
	ipt.mu.Lock()
	defer ipt.mu.Unlock()

	if ip == "*" {
		ip = "." + ip
	}
	delete(ipt.iptable, ip)
}

// Refresh used to refresh the table.
func (ipt *IPTable) Refresh() error {
	ipt.log.Warning("proxy.iptable.refresh:%+v", ipt.conf.IPS)
	ipt.mu.Lock()
	defer ipt.mu.Unlock()

	ipt.iptable = make(map[string]interface{})
	if ipt.conf.IPS != nil {
		for _, ip := range ipt.conf.IPS {
			if err := addToIPTable(ipt, ip); err != nil {
				return err
			}
		}
	}
	return nil
}

// Check used to check whether the ip is in ip table or not.
func (ipt *IPTable) Check(address string) bool {
	ipt.mu.Lock()
	defer ipt.mu.Unlock()

	// if address is in iptable[address], just return
	_, ok := ipt.iptable[address]
	if ok {
		return true
	}
	// check if address is match with ip regexp
	for _, ip := range ipt.iptable {
		switch ip.(type) {
		case *regexp.Regexp:
			if ip.(*regexp.Regexp).MatchString(address) {
				return true
			}
		}
	}
	return false
}
