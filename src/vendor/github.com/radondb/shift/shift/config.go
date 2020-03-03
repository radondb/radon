/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package shift

// Use flavor for different target cluster
const (
	ToMySQLFlavor   = "mysql"
	ToMariaDBFlavor = "mariadb"
	ToRadonDBFlavor = "radondb"
)

type Config struct {
	ToFlavor string

	From         string
	FromUser     string
	FromPassword string
	FromDatabase string
	FromTable    string

	To         string
	ToUser     string
	ToPassword string
	ToDatabase string
	ToTable    string

	Rebalance              bool
	Cleanup                bool
	MySQLDump              string
	Threads                int
	Behinds                int
	RadonURL               string
	Checksum               bool
	WaitTimeBeforeChecksum int // seconds
}
