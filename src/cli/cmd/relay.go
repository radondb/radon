/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package cmd

import (
	"fmt"
	"net/http"
	"time"

	"xbase"

	"github.com/spf13/cobra"
)

func NewRelayCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "relay",
		Short: "show/enable/disable relay worker",
	}
	cmd.AddCommand(NewRelayStatusCommand())
	cmd.AddCommand(NewRelayInfosCommand())
	cmd.AddCommand(NewRelayStartCommand())
	cmd.AddCommand(NewRelayStopCommand())
	cmd.AddCommand(NewRelayParallelTypeCommand())
	cmd.AddCommand(NewRelayResetCommand())
	cmd.AddCommand(NewRelayResetToNowCommand())
	cmd.AddCommand(NewRelayMaxWorkersCommand())
	cmd.AddCommand(NewRelayNowCommand())
	return cmd
}

func NewRelayStatusCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "show relay status",
		Run:   relayStatusCommand,
	}
	return cmd
}

func relayStatusCommand(cmd *cobra.Command, args []string) {
	relayUrl := "http://127.0.0.1:8080/v1/relay/status"
	resp, err := xbase.HTTPGet(relayUrl)
	if err != nil {
		log.Panicf("error:%+v", err)
	}
	fmt.Printf(resp)
}

func NewRelayInfosCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "infos",
		Short: "show relay all infos",
		Run:   relayInfosCommand,
	}
	return cmd
}

func relayInfosCommand(cmd *cobra.Command, args []string) {
	relayUrl := "http://127.0.0.1:8080/v1/relay/infos"
	resp, err := xbase.HTTPGet(relayUrl)
	if err != nil {
		log.Panicf("error:%+v", err)
	}
	fmt.Printf(resp)
}

func setRelay(url string) {
	resp, cleanup, err := xbase.HTTPPut(url, nil)
	defer cleanup()

	if err != nil {
		log.Panicf("error:%+v", err)
	}

	if resp == nil || resp.StatusCode != http.StatusOK {
		log.Panicf("radoncli.set.relay.url[%s].response.error:%+s", url, xbase.HTTPReadBody(resp))
	}
}

func NewRelayStartCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "start the relay worker",
		Run:   relayStartCommand,
	}
	return cmd
}

func relayStartCommand(cmd *cobra.Command, args []string) {
	relayUrl := "http://127.0.0.1:8080/v1/relay/start"
	setRelay(relayUrl)
}

func NewRelayStopCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stop",
		Short: "stop the relay worker",
		Run:   relayStopCommand,
	}
	return cmd
}

func relayStopCommand(cmd *cobra.Command, args []string) {
	relayUrl := "http://127.0.0.1:8080/v1/relay/stop"
	setRelay(relayUrl)
}

func setParallelType(url string, t int32) {
	type request struct {
		Type int32 `json:"type"`
	}

	req := &request{
		Type: t,
	}
	resp, cleanup, err := xbase.HTTPPut(url, &req)
	defer cleanup()

	if err != nil {
		log.Panicf("error:%+v", err)
	}

	if resp == nil || resp.StatusCode != http.StatusOK {
		log.Panicf("radoncli.set.parallel.type.to.[%v].url[%s].response.error:%+s", t, url, xbase.HTTPReadBody(resp))
	}
}

func NewRelayParallelTypeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "paralleltype",
		Short: "parallel type, 0:turn off parallel relay, 1:same events type can parallel(default), 2:all events type can parallel",
		Run:   relayParallelTypeCommand,
	}
	cmd.Flags().IntVar(&localFlags.parallelType, "type", 1, "")
	return cmd
}

func relayParallelTypeCommand(cmd *cobra.Command, args []string) {
	url := "http://127.0.0.1:8080/v1/relay/paralleltype"
	setParallelType(url, int32(localFlags.parallelType))
}

func relayResetGTID(gtid int64) {
	if gtid < 1514254947594569594 {
		log.Panicf("gtid[%v].less.than[1514254947594569594].should.be.UTC().UnixNano()", gtid)
	}

	relayUrl := "http://127.0.0.1:8080/v1/relay/reset"
	type request struct {
		GTID int64 `json:"gtid"`
	}

	req := &request{
		GTID: gtid,
	}
	resp, cleanup, err := xbase.HTTPPost(relayUrl, &req)
	defer cleanup()

	if err != nil {
		log.Panicf("error:%+v", err)
	}

	if resp == nil || resp.StatusCode != http.StatusOK {
		log.Panicf("radoncli.set.relay.to.[%v].url[%s].response.error:%+s", req, relayUrl, xbase.HTTPReadBody(resp))
	}
	log.Info("reset.relay.gtid.to[%v]", gtid)
}

func NewRelayResetCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reset",
		Short: "reset the relay worker GTID",
		Run:   relayResetCommand,
	}
	cmd.Flags().Int64Var(&localFlags.gtid, "gtid", 0, "--gtid=[timestamp(UTC().UnixNano())]")
	return cmd
}

func relayResetCommand(cmd *cobra.Command, args []string) {
	relayResetGTID(localFlags.gtid)
}

func NewRelayResetToNowCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resettonow",
		Short: "reset the relay worker GTID to time.NOW().UTC().UnixNano()",
		Run:   relayResetToNowCommand,
	}
	return cmd
}

func relayResetToNowCommand(cmd *cobra.Command, args []string) {
	relayResetGTID(time.Now().UTC().UnixNano())
}

func NewRelayMaxWorkersCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workers",
		Short: "Set the max relay parallel workers",
		Run:   relayMaxWorkersCommand,
	}
	cmd.Flags().IntVar(&localFlags.maxWorkers, "max", 0, "--max=[1, 1024]")
	return cmd
}

func relayMaxWorkersCommand(cmd *cobra.Command, args []string) {
	relayUrl := "http://127.0.0.1:8080/v1/relay/workers"
	type request struct {
		Workers int `json:"workers"`
	}

	req := &request{
		Workers: localFlags.maxWorkers,
	}
	resp, cleanup, err := xbase.HTTPPost(relayUrl, &req)
	defer cleanup()

	if err != nil {
		log.Panicf("error:%+v", err)
	}

	if resp == nil || resp.StatusCode != http.StatusOK {
		log.Panicf("radoncli.relay.set.max.parallel.worker.to.[%v].url[%s].response.error:%+s", req, relayUrl, xbase.HTTPReadBody(resp))
	}
}

func NewRelayNowCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "now",
		Short: "returns the Now().UTC().UnixNano()",
		Run:   relayNowCommand,
	}
	return cmd
}

func relayNowCommand(cmd *cobra.Command, args []string) {
	log.Info("Now().UTC().UnixNano():%v", time.Now().UTC().UnixNano())
}
