/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package builder

// ChildType type.
type ChildType string

const (
	// ChildTypeOrderby enum.
	ChildTypeOrderby ChildType = "ChildTypeOrderby"

	// ChildTypeLimit enum.
	ChildTypeLimit ChildType = "ChildTypeLimit"

	// ChildTypeAggregate enum.
	ChildTypeAggregate ChildType = "ChildTypeAggregate"
)

// ChildPlan interface.
type ChildPlan interface {
	Build() error
	Type() ChildType
	JSON() string
}
