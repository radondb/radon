/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package router

import (
	"config"

	"github.com/pkg/errors"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

// SingleRange for Segment.Range.
type SingleRange struct {
	str string
}

// String returns ''.
func (r *SingleRange) String() string {
	return r.str
}

// Less impl.
func (r *SingleRange) Less(b KeyRange) bool {
	return false
}

// Single for single table router.
type Single struct {
	log *xlog.Log

	// single method.
	typ MethodType

	// table config.
	conf *config.TableConfig

	// Segments slice.
	Segments []Segment `json:",omitempty"`
}

// NewSingle creates new global.
func NewSingle(log *xlog.Log, conf *config.TableConfig) *Single {
	return &Single{
		log:      log,
		conf:     conf,
		typ:      methodTypeSingle,
		Segments: make([]Segment, 0, 16),
	}
}

// Build used to build Segments from schema config.
func (s *Single) Build() error {
	if s.conf == nil {
		return errors.New("table.config..can't.be.nil")
	}
	for _, part := range s.conf.Partitions {
		partition := Segment{
			Table:   part.Table,
			Backend: part.Backend,
			Range: &SingleRange{
				str: "",
			},
		}
		s.Segments = append(s.Segments, partition)
	}

	return nil
}

// Lookup used to lookup partition(s).
func (s *Single) Lookup(start *sqlparser.SQLVal, end *sqlparser.SQLVal) ([]Segment, error) {
	return s.Segments, nil
}

// Type returns the global type.
func (s *Single) Type() MethodType {
	return s.typ
}

// GetIndex returns index based on sqlval.
func (s *Single) GetIndex(sqlval *sqlparser.SQLVal) (int, error) {
	return 0, nil
}

// GetSegments returns Segments based on index.
func (s *Single) GetSegments() []Segment {
	return s.Segments
}

func (s *Single) GetSegment(index int) (Segment, error) {
	if index >= len(s.Segments) {
		return Segment{}, errors.Errorf("single.getsegment.index.[%d].out.of.range", index)
	}
	return s.Segments[index], nil
}
