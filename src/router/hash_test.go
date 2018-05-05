/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package router

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/sqlparser"
	"github.com/xelabs/go-mysqlstack/xlog"
)

var (
	_mockHashSlots = 4096
)

func TestHash(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	hash := NewHash(log, _mockHashSlots, MockTableAConfig())
	{
		err := hash.Build()
		assert.Nil(t, err)
		assert.Equal(t, string(hash.Type()), methodTypeHash)
		assert.Equal(t, hash.slots, 4096)
		assert.Equal(t, len(hash.partitions), 4096)
	}

	{
		err := hash.Clear()
		assert.Nil(t, err)
		err = hash.Build()
		assert.Nil(t, err)
	}
}

func TestHashOverlap(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	hash := NewHash(log, _mockHashSlots, MockTableOverlapConfig())
	err := hash.Build()
	{
		want := "hash.partition.segment[7-9].overlapped[7]"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestHashInvalid(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	hash := NewHash(log, _mockHashSlots, MockTableInvalidConfig())
	err := hash.Build()
	{
		want := "hash.partition.segment.malformed[8-x].end.can.not.parser.to.int"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestHashGreaterThan(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	hash := NewHash(log, _mockHashSlots, MockTableGreaterThanConfig())
	err := hash.Build()
	{
		want := "hash.partition.segment.malformed[10-8].start[10]>=end[8]"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestHash64(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	hash := NewHash(log, _mockHashSlots, MockTable64Config())
	err := hash.Build()
	{
		want := "hash.partition.last.segment[64].upper.bound.must.be[4096]"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestHashLookup(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	hash := NewHash(log, _mockHashSlots, MockTableAConfig())
	{
		err := hash.Build()
		assert.Nil(t, err)
	}

	intVal := sqlparser.NewIntVal([]byte("-65536"))
	floatVal := sqlparser.NewFloatVal([]byte("65536.99999"))
	strVal := sqlparser.NewStrVal([]byte("shardkey"))
	{
		parts, err := hash.Lookup(strVal, strVal)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(parts))
	}

	// int
	{
		parts, err := hash.Lookup(intVal, intVal)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(parts))
		assert.Equal(t, "A8", parts[0].Table)
		assert.Equal(t, "backend8", parts[0].Backend)
	}

	// float
	{
		parts, err := hash.Lookup(floatVal, floatVal)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(parts))
		assert.Equal(t, "A8", parts[0].Table)
		assert.Equal(t, "backend8", parts[0].Backend)
	}

	// str
	{
		parts, err := hash.Lookup(strVal, strVal)
		assert.Nil(t, err)
		assert.Equal(t, 1, len(parts))
		assert.Equal(t, "A8", parts[0].Table)
		assert.Equal(t, "backend8", parts[0].Backend)
	}

	// [nil, endKey]
	{
		parts, err := hash.Lookup(nil, strVal)
		assert.Nil(t, err)
		assert.Equal(t, 4, len(parts))
	}

	// [nil, nil]
	{
		parts, err := hash.Lookup(nil, nil)
		assert.Nil(t, err)
		assert.Equal(t, 4, len(parts))
	}

	// [start, end)
	{
		s := sqlparser.NewIntVal([]byte("16"))
		e := sqlparser.NewIntVal([]byte("17"))

		parts, err := hash.Lookup(s, e)
		assert.Nil(t, err)
		assert.Equal(t, 4, len(parts))
	}
}

func TestHashBuildError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	{
		hash := NewHash(log, _mockHashSlots, MockTableSegmentErr1Config())
		err := hash.Build()
		want := "hash.partition.segment.malformed[0]"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	{
		hash := NewHash(log, _mockHashSlots, MockTableSegmentStartErrConfig())
		err := hash.Build()
		want := "hash.partition.segment.malformed[x-0].start.can.not.parser.to.int"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	{
		hash := NewHash(log, _mockHashSlots, MockTableSegmentEndErrConfig())
		err := hash.Build()
		want := "hash.partition.segment.malformed[0-x].end.can.not.parser.to.int"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestHashLookupError(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	hash := NewHash(log, _mockHashSlots, MockTableAConfig())
	{
		err := hash.Build()
		assert.Nil(t, err)
	}

	intVal := sqlparser.NewIntVal([]byte("65536"))
	strVal := sqlparser.NewStrVal([]byte("shardkey"))
	hexVal := sqlparser.NewHexNum([]byte("3.1415926"))
	{
		_, err := hash.Lookup(strVal, intVal)
		want := "hash.lookup.key.type.must.be.same:[0!=1]"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	{
		intVal := sqlparser.NewIntVal([]byte("65536x"))
		_, err := hash.Lookup(intVal, intVal)
		want := "hash.lookup.start.key.parser.uint64.error:[strconv.ParseInt: parsing \"65536x\": invalid syntax]"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	{
		floatVal := sqlparser.NewFloatVal([]byte("65536.x"))
		_, err := hash.Lookup(floatVal, floatVal)
		want := "hash.lookup.start.key.parser.float.error:[strconv.ParseFloat: parsing \"65536.x\": invalid syntax]"
		got := err.Error()
		assert.Equal(t, want, got)
	}

	{
		_, err := hash.Lookup(hexVal, hexVal)
		want := "hash.unsupported.key.type:[3]"
		got := err.Error()
		assert.Equal(t, want, got)
	}
}

func TestHashLookupBench(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	hash := NewHash(log, _mockHashSlots, MockTableAConfig())
	{
		err := hash.Build()
		assert.Nil(t, err)
	}

	{
		N := 1000000
		now := time.Now()
		for i := 0; i < N; i++ {
			intVal := sqlparser.NewIntVal([]byte(fmt.Sprintf("%d", i)))
			_, err := hash.Lookup(intVal, intVal)
			assert.Nil(t, err)
		}

		took := time.Since(now)
		fmt.Printf(" LOOP\t%v COST %v, avg:%v/s\n", N, took, (int64(N)/(took.Nanoseconds()/1e6))*1000)
	}
}
