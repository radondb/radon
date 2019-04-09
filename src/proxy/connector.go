/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package proxy

import (
	"strings"
)

// isConnectorFilter -- used to check the query is JDBC/Connector set.
func (spanner *Spanner) isConnectorFilter(query string) bool {
	return strings.HasPrefix(query, "/*") || strings.HasPrefix(query, "SET NAMES")
}
