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

)

func init() {
	prometheus.MustRegister(clientConnectionNum)
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

