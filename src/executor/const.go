/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package executor

const (
	// RowNumLimit mains the result row's limit to deside which
	// join methods to choose. <1000 simpleBNJoin, else merge join.
	RowNumLimit = 1000
)
