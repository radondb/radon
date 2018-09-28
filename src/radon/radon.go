/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
	"time"

	"build"
	"config"
	"ctl"
	"monitor"
	"proxy"

	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	flagConf   string
	fcpu       *os.File
	pprofCpuOn = flag.Bool("pcpu", false, "is cpu prof enable, default false")
)

func init() {
	flag.StringVar(&flagConf, "c", "", "radon config file")
	flag.StringVar(&flagConf, "config", "", "radon config file")
}

func usage() {
	fmt.Println("Usage: " + os.Args[0] + " [-c|--config] <radon-config-file>")
}

func startPprof() {
	nowStr := time.Now().Format(time.RFC3339)
	if *pprofCpuOn {
		cpuFile := "pprof_cpu_" + nowStr
		fcpu, err := os.Create(cpuFile)
		if err != nil {
			fmt.Println("start pprof cpu failed", err)
			os.Exit(1)
		}

		pprof.StartCPUProfile(fcpu)
		fmt.Println("[pprof cpu]:\t" + cpuFile)
	}
}

func stopPprof() {
	if *pprofCpuOn {
		pprof.StopCPUProfile()
		fcpu.Close()
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log := xlog.NewStdLog(xlog.Level(xlog.DEBUG))

	build := build.GetInfo()
	fmt.Printf("radon:[%+v]\n", build)

	// config
	flag.Usage = func() { usage() }
	flag.Parse()
	if flagConf == "" {
		usage()
		os.Exit(0)
	}

	conf, err := config.LoadConfig(flagConf)
	if err != nil {
		log.Panic("radon.load.config.error[%v]", err)
	}
	log.SetLevel(conf.Log.Level)

	// pprof
	startPprof()
	defer stopPprof()

	// Monitor
	monitor.Start(log, conf)

	// Proxy.
	proxy := proxy.NewProxy(log, flagConf, conf)
	proxy.Start()

	// Admin portal.
	admin := ctl.NewAdmin(log, proxy)
	admin.Start()

	// Handle SIGINT and SIGTERM.
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	log.Info("radon.signal:%+v", <-ch)

	// Stop the proxy and httpserver.
	proxy.Stop()
	admin.Stop()
}
