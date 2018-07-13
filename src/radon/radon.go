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
	"proxy"

	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	flag_conf string
)

func init() {
	flag.StringVar(&flag_conf, "c", "", "radon config file")
	flag.StringVar(&flag_conf, "config", "", "radon config file")
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
	if flag_conf == "" {
		usage()
		os.Exit(0)
	}

	conf, err := config.LoadConfig(flag_conf)
	if err != nil {
		log.Panic("radon.load.config.error[%v]", err)
	}
	log.SetLevel(conf.Log.Level)

	// Proxy.
	proxy := proxy.NewProxy(log, flag_conf, conf)
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
