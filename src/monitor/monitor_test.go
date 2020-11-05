/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package monitor

import (
	"testing"

	"config"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/xelabs/go-mysqlstack/xlog"
)

func TestClientConnectionIncDec(t *testing.T) {
	user := "andy"
	ClientConnectionInc(user)

	var m dto.Metric
	g, _ := clientConnectionNum.GetMetricWithLabelValues(user)
	err := g.Write(&m)
	assert.Nil(t, err)
	v := m.GetGauge().GetValue()

	assert.EqualValues(t, 1, v)

	ClientConnectionDec(user)

	g, _ = clientConnectionNum.GetMetricWithLabelValues(user)
	err = g.Write(&m)
	assert.Nil(t, err)
	v = m.GetGauge().GetValue()

	assert.EqualValues(t, 0, v)
}

func TestBackendConnectionIncDec(t *testing.T) {
	address := "192.168.0.2:3306"
	BackendConnectionInc(address)

	var m dto.Metric
	g, _ := backendConnectionNum.GetMetricWithLabelValues(address)
	err := g.Write(&m)
	assert.Nil(t, err)
	v := m.GetGauge().GetValue()

	assert.EqualValues(t, 1, v)

	BackendConnectionDec(address)

	g, _ = backendConnectionNum.GetMetricWithLabelValues(address)
	err = g.Write(&m)
	assert.Nil(t, err)
	v = m.GetGauge().GetValue()

	assert.EqualValues(t, 0, v)
}

func TestQueryTotalCounterInc(t *testing.T) {
	command := "Select"
	result := "OK"
	QueryTotalCounterInc(command, result)
	QueryTotalCounterInc(command, result)

	var m dto.Metric
	g, _ := queryTotalCounter.GetMetricWithLabelValues(command, result)
	err := g.Write(&m)
	assert.Nil(t, err)
	v := m.GetCounter().GetValue()
	assert.EqualValues(t, 2, v)

	command = "Unsupport"
	result = "Error"
	QueryTotalCounterInc(command, result)

	g, _ = queryTotalCounter.GetMetricWithLabelValues(command, result)
	err = g.Write(&m)
	assert.Nil(t, err)
	v = m.GetCounter().GetValue()

	assert.EqualValues(t, 1, v)
}

func TestBackendIncDec(t *testing.T) {
	getBackendNum := func(btype string) float64 {
		var m dto.Metric
		g, _ := backendNum.GetMetricWithLabelValues(btype)
		err := g.Write(&m)
		assert.Nil(t, err)
		return m.GetGauge().GetValue()
	}

	backend := "backend"
	BackendInc(backend)
	v1 := getBackendNum(backend)
	assert.EqualValues(t, 1, v1)

	BackendSet(backend, 2)
	v1 = getBackendNum(backend)
	assert.EqualValues(t, 2, v1)

	BackendDec(backend)
	v1 = getBackendNum(backend)
	assert.EqualValues(t, 1, v1)

	BackendSet(backend, 0)
	v1 = getBackendNum(backend)
	assert.EqualValues(t, 0, v1)
}

func TestDiskUsageSet(t *testing.T) {
	v := 0.35

	DiskUsageSet(v)

	var m dto.Metric
	g, _ := diskUsage.GetMetricWithLabelValues("percent")
	err := g.Write(&m)
	assert.Nil(t, err)
	r := m.GetGauge().GetValue()

	assert.EqualValues(t, v, r)
}

func TestSlowQueryTotalCounterInc(t *testing.T) {
	// sql supported
	command := "Select"
	result := "OK"
	SlowQueryTotalCounterInc(command, result)
	SlowQueryTotalCounterInc(command, result)

	var m dto.Metric
	g, _ := queryTotalCounter.GetMetricWithLabelValues(command, result)
	err := g.Write(&m)
	assert.Nil(t, err)
	v := m.GetCounter().GetValue()
	assert.EqualValues(t, 2, v)

	// sql not supported
	command = "Unsupport"
	result = "Error"
	SlowQueryTotalCounterInc(command, result)

	g, _ = queryTotalCounter.GetMetricWithLabelValues(command, result)
	err = g.Write(&m)
	assert.Nil(t, err)
	v = m.GetCounter().GetValue()

	assert.EqualValues(t, 1, v)
}

func TestPeerNum(t *testing.T) {
	PeerNumSet(1)

	var m dto.Metric
	err := peerNum.Write(&m)
	assert.Nil(t, err)
	v := m.GetGauge().GetValue()

	assert.EqualValues(t, 1, v)

	PeerNumInc()
	err = peerNum.Write(&m)
	assert.Nil(t, err)
	v = m.GetGauge().GetValue()

	assert.EqualValues(t, 2, v)

	PeerNumDec()
	err = peerNum.Write(&m)
	assert.Nil(t, err)
	v = m.GetGauge().GetValue()

	assert.EqualValues(t, 1, v)
}

func TestMonitorStart(t *testing.T) {
	log := xlog.NewStdLog(xlog.Level(xlog.PANIC))
	var conf config.Config
	conf.Proxy = config.DefaultProxyConfig()
	conf.Audit = config.DefaultAuditConfig()
	conf.Router = config.DefaultRouterConfig()
	conf.Log = config.DefaultLogConfig()
	conf.Monitor = config.DefaultMonitorConfig()
	Start(log, &conf)
}
