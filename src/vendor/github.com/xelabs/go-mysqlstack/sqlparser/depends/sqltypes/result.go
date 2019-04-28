// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sqltypes

import (
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

// ResultState enum.
type ResultState int

const (
	// RStateNone enum.
	RStateNone ResultState = iota
	// RStateFields enum.
	RStateFields
	// RStateRows enum.
	RStateRows
	// RStateFinished enum.
	RStateFinished
)

// Result represents a query result.
type Result struct {
	Fields       []*querypb.Field      `json:"fields"`
	RowsAffected uint64                `json:"rows_affected"`
	InsertID     uint64                `json:"insert_id"`
	Warnings     uint16                `json:"warnings"`
	Rows         [][]Value             `json:"rows"`
	Extras       *querypb.ResultExtras `json:"extras"`
	State        ResultState
}

// ResultStream is an interface for receiving Result. It is used for
// RPC interfaces.
type ResultStream interface {
	// Recv returns the next result on the stream.
	// It will return io.EOF if the stream ended.
	Recv() (*Result, error)
}

// Repair fixes the type info in the rows
// to conform to the supplied field types.
func (result *Result) Repair(fields []*querypb.Field) {
	// Usage of j is intentional.
	for j, f := range fields {
		for _, r := range result.Rows {
			if r[j].typ != Null {
				r[j].typ = f.Type
			}
		}
	}
}

// Copy creates a deep copy of Result.
func (result *Result) Copy() *Result {
	out := &Result{
		InsertID:     result.InsertID,
		RowsAffected: result.RowsAffected,
	}
	if result.Fields != nil {
		fieldsp := make([]*querypb.Field, len(result.Fields))
		fields := make([]querypb.Field, len(result.Fields))
		for i, f := range result.Fields {
			fields[i] = *f
			fieldsp[i] = &fields[i]
		}
		out.Fields = fieldsp
	}
	if result.Rows != nil {
		rows := make([][]Value, len(result.Rows))
		for i, r := range result.Rows {
			rows[i] = make([]Value, len(r))
			totalLen := 0
			for _, c := range r {
				totalLen += len(c.val)
			}
			arena := make([]byte, 0, totalLen)
			for j, c := range r {
				start := len(arena)
				arena = append(arena, c.val...)
				rows[i][j] = MakeTrusted(c.typ, arena[start:start+len(c.val)])
			}
		}
		out.Rows = rows
	}
	return out
}

// StripFieldNames will return a new Result that has the same Rows,
// but the Field objects will have their Name emptied.  Note we don't
// proto.Copy each Field for performance reasons, but we only copy the
// individual fields.
func (result *Result) StripFieldNames() *Result {
	if len(result.Fields) == 0 {
		return result
	}
	r := *result
	r.Fields = make([]*querypb.Field, len(result.Fields))
	newFieldsArray := make([]querypb.Field, len(result.Fields))
	for i, f := range result.Fields {
		r.Fields[i] = &newFieldsArray[i]
		newFieldsArray[i].Type = f.Type
	}
	return &r
}

// AppendResult will combine the Results Objects of one result
// to another result.Note currently it doesn't handle cases like
// if two results have different fields.We will enhance this function.
func (result *Result) AppendResult(src *Result) {
	if src.RowsAffected == 0 && len(src.Fields) == 0 {
		return
	}
	if result.Fields == nil {
		result.Fields = src.Fields
	}
	result.RowsAffected += src.RowsAffected
	if src.InsertID != 0 {
		result.InsertID = src.InsertID
	}
	if len(src.Rows) != 0 {
		result.Rows = append(result.Rows, src.Rows...)
	}
}
