/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package monitor

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	webMonitorPort = "13308"
	webMonitorAddr = "0.0.0.0"
	webMonitorURL  = "/metrics"

	clientConnectionNum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "connection_number_client",
			Help: "client connection Number",
		},
		[]string{"user"},
	)

	backendConnectionNum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "connection_number_backend",
			Help: "backend connection Number",
		},
		[]string{"address"},
	)

	queryTotalCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "query_total",
			Help: "Counter of queries.",
		},
		[]string{"command", "result"},
	)

	backendNum = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "backend_number",
			Help: "backend Number",
		},
		[]string{"type"},
	)

	diskUsage = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "disk_usage",
			Help: "disk usage",
		},
		[]string{"description"},
	)
)

func init() {
	prometheus.MustRegister(clientConnectionNum)
	prometheus.MustRegister(backendConnectionNum)
	prometheus.MustRegister(queryTotalCounter)
	prometheus.MustRegister(backendNum)
	prometheus.MustRegister(diskUsage)
}

// Start monitor
func Start(addr, port string) {
	if addr != "" {
		webMonitorAddr = addr
	}
	if port != "" {
		webMonitorPort = port
	}
	fmt.Printf("[prometheus metrics]:\thttp://{%s}:%s%s\n",
		webMonitorAddr, webMonitorPort, webMonitorURL)
	fmt.Printf("[pprof web]:\t\thttp://{%s}:%s/debug/pprof/\n",
		webMonitorAddr, webMonitorPort)
	http.Handle(webMonitorURL, promhttp.Handler())
	go http.ListenAndServe(webMonitorAddr+":"+webMonitorPort, nil)
}

// ClientConnectionInc add 1
func ClientConnectionInc(user string) {
	clientConnectionNum.WithLabelValues(user).Inc()
}

// ClientConnectionDec dec 1
func ClientConnectionDec(user string) {
	clientConnectionNum.WithLabelValues(user).Dec()
}

// BackendConnectionInc add 1
func BackendConnectionInc(address string) {
	backendConnectionNum.WithLabelValues(address).Inc()
}

// BackendConnectionDec dec 1
func BackendConnectionDec(address string) {
	backendConnectionNum.WithLabelValues(address).Dec()
}

//QueryTotalCounterInc add 1
func QueryTotalCounterInc(command string, result string) {
	queryTotalCounter.WithLabelValues(command, result).Inc()
}

// BackendInc add 1
func BackendInc(btype string) {
	backendNum.WithLabelValues(btype).Inc()
}

// BackendDec dec 1
func BackendDec(btype string) {
	backendNum.WithLabelValues(btype).Dec()
}

// DiskUsageSet set usage of disk
func DiskUsageSet(v float64) {
	diskUsage.WithLabelValues("percent").Set(v)
}
