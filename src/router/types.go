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
	MethodTypeHash   = "HASH"
	MethodTypeGlobal = "GLOBAL"
	MethodTypeSingle = "SINGLE"
	MethodTypeList   = "LIST"
)
