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
	"syscall"

	"build"
	"config"
	"ctl"
	"monitor"
	"proxy"

	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	flagConf string
)

func init() {
	flag.StringVar(&flagConf, "c", "", "radon config file")
	flag.StringVar(&flagConf, "config", "", "radon config file")
}

func usage() {
	fmt.Println("Usage: " + os.Args[0] + " [-c|--config] <radon-config-file>")
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

	// Monitor
	monitor.Start(log, conf)

	// Proxy.
	proxy := proxy.NewProxy(log, flagConf, build.Tag, conf)
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
