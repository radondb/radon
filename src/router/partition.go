/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package router

import (
	"github.com/xelabs/go-mysqlstack/sqlparser"
)

// KeyRange tuple.
type KeyRange interface {
	String() string
	Less(KeyRange) bool
}

// Segments slice.
type Segments []Segment

// Len impl.
func (q Segments) Len() int { return len(q) }

// Segments impl.
func (q Segments) Swap(i, j int) { q[i], q[j] = q[j], q[i] }

// Less impl.
func (q Segments) Less(i, j int) bool {
	return q[i].Range.Less(q[j].Range)
}

// Segment tuple.
type Segment struct {
	// Segment table name.
	Table string `json:",omitempty"`
	// Segment backend name.
	Backend string `json:",omitempty"`
	// key range of this segment.
	Range KeyRange `json:",omitempty"`

	// partition list value.
	ListValue string `json:",omitempty"`
}

// Partition interface.
type Partition interface {
	Build() error
	Type() MethodType
	Lookup(start *sqlparser.SQLVal, end *sqlparser.SQLVal) ([]Segment, error)
	GetIndex(sqlval *sqlparser.SQLVal) (int, error)
	GetSegments() []Segment
	GetSegment(index int) (Segment, error)
}
