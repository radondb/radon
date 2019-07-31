/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package shift

type Config struct {
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

	Cleanup                bool
	MySQLDump              string
	Threads                int
	Behinds                int
	RadonURL               string
	Checksum               bool
	WaitTimeBeforeChecksum int // seconds
}
