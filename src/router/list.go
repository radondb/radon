/*
 * Radon
 *
 * Copyright 2018-2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package router

import (
	"bytes"
	"sort"

	"config"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/sqlparser/depends/common"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// ListRange for Segment.Range.
type ListRange struct {
	str string
}

// String returns start-end info.
func (r *ListRange) String() string {
	return r.str
}

// Less impl.
func (r *ListRange) Less(b KeyRange) bool {
	return false
}

// List tuple.
type List struct {
	log *xlog.Log

	// slots
	slots int

	// method
	typ MethodType

	// table config
	conf *config.TableConfig

	// Partition map
	Segments []Segment `json:",omitempty"`
}

// NewList creates new list.
func NewList(log *xlog.Log, conf *config.TableConfig) *List {
	return &List{
		log:      log,
		conf:     conf,
		typ:      methodTypeList,
		Segments: make([]Segment, 0, 16),
	}
}

// Build used to build list bitmap from schema config
func (list *List) Build() error {
	for _, part := range list.conf.Partitions {
		partition := Segment{
			Table:     part.Table,
			Backend:   part.Backend,
			ListValue: part.ListValue,
			Range: &ListRange{
				str: "",
			},
		}
		// Segments
		list.Segments = append(list.Segments, partition)
	}
	return nil
}

// Clear used to clean partitions
func (list *List) Clear() error {
	return nil
}

// Lookup used to lookup partition(s) through the sharding-key range
// List.Lookup only supports the type uint64/string
func (list *List) Lookup(start *sqlparser.SQLVal, end *sqlparser.SQLVal) ([]Segment, error) {
	// if open interval we returns all partitions
	if start == nil || end == nil {
		return list.Segments, nil
	}

	// Check item types.
	if start.Type != end.Type {
		return nil, errors.Errorf("list.lookup.key.type.must.be.same:[%v!=%v]", start.Type, end.Type)
	}

	// List just handle the equal
	if bytes.Equal(start.Val, end.Val) {
		idx, err := list.GetIndex(start)
		if err != nil {
			return nil, err
		}
		return []Segment{list.Segments[idx]}, nil
	}

	sort.Sort(Segments(list.Segments))
	return list.Segments, nil
}

// Type returns the list type.
func (list *List) Type() MethodType {
	return list.typ
}

// GetIndex returns index based on sqlval.
func (list *List) GetIndex(sqlval *sqlparser.SQLVal) (int, error) {
	idx := -1
	valStr := common.BytesToString(sqlval.Val)
	for idx, segment := range list.Segments {
		if segment.ListValue == valStr {
			return idx, nil
		}
	}
	return idx, errors.Errorf("Table has no partition for value %v", valStr)
}

// GetSegments returns Segments based on index.
func (list *List) GetSegments() []Segment {
	return list.Segments
}

// GetSegment ...
func (list *List) GetSegment(index int) (Segment, error) {
	if index >= len(list.Segments) {
		return Segment{}, errors.Errorf("single.getsegment.index.[%d].out.of.range", index)
	}
	return list.Segments[index], nil
}
