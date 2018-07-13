/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package main

import (
	"fmt"
	"os"

	"cli/cmd"

	"github.com/spf13/cobra"
)

const (
	cliName        = "radoncli"
	cliDescription = "A simple command line client for radon"
)

var (
	rootCmd = &cobra.Command{
		Use:        cliName,
		Short:      cliDescription,
		SuggestFor: []string{"radoncli"},
	}
)

func init() {
	rootCmd.AddCommand(cmd.NewVersionCommand())
	rootCmd.AddCommand(cmd.NewReadonlyCommand())
	rootCmd.AddCommand(cmd.NewTwopcCommand())
	rootCmd.AddCommand(cmd.NewDebugCommand())
	rootCmd.AddCommand(cmd.NewRelayCommand())
	rootCmd.AddCommand(cmd.NewBackupCommand())
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}
