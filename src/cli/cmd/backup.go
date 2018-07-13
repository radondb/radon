/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package cmd

import (
	"encoding/json"
	"fmt"

	"xbase"

	"github.com/spf13/cobra"
	streamer "github.com/xelabs/go-mydumper/src/common"
)

var (
	database     = ""
	radonPort    = 3306
	backupEngine = "tokudb"
)

func NewBackupCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backup",
		Short: "rebuild the backup datas",
	}
	cmd.AddCommand(NewBackupRebuildCommand())
	return cmd
}

func NewBackupRebuildCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "rebuild",
		Example: "rebuild --database=DB",
		Run:     backupRebuildCommand,
	}
	cmd.PersistentFlags().IntVar(&radonPort, "radon-port", 3306, "--radon-port=[port]")
	cmd.PersistentFlags().StringVar(&backupEngine, "backup-engine", "tokudb", "--backup-engine=[engine]")
	cmd.PersistentFlags().StringVar(&database, "database", "", "--database=[db]")
	return cmd
}

func backupRebuildCommand(cmd *cobra.Command, args []string) {
	if database == "" {
		log.Panicf("database.cant.be.null")
	}

	// First to stop the relay.
	url := "http://127.0.0.1:8080/v1/relay/stop"
	setRelay(url)
	log.Info("backup.rebuild.stop.the.relay...")

	// Get the backup address/user/pwd.
	type backupConfig struct {
		Address  string `json:"address"`
		User     string `json:"user"`
		Password string `json:"password"`
	}
	url = "http://127.0.0.1:8080/v1/radon/backupconfig"
	body, err := xbase.HTTPGet(url)
	if err != nil {
		log.Panic("backup.rebuild.get.backup.config.error:%v", err)
	}
	log.Info("get.the.backup.config:%v", body)

	backConf := &backupConfig{}
	err = json.Unmarshal([]byte(body), backConf)
	if err != nil {
		log.Panic("backup.rebuild.unmarshal.config[%s].error:%v", body, err)
	}

	streamArgs := &streamer.Args{
		User:            "root",
		Password:        "",
		Address:         fmt.Sprintf("127.0.0.1:%d", radonPort),
		ToUser:          backConf.User,
		ToPassword:      backConf.Password,
		ToAddress:       backConf.Address,
		ToEngine:        backupEngine,
		Database:        database,
		ToDatabase:      database,
		Threads:         32,
		StmtSize:        1000000,
		IntervalMs:      10 * 1000,
		OverwriteTables: true,
	}
	streamer.Streamer(log, streamArgs)
}
