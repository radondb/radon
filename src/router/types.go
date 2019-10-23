/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package router

// MethodType type.
type MethodType string

const (
	// methodTypeHash type.
	methodTypeHash   = "HASH"
	methodTypeGlobal = "GLOBAL"
	methodTypeSingle = "SINGLE"
	methodTypeList   = "LIST"
)
