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

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

func TestClientConnectionIncDec(t *testing.T) {
	user := "andy"
	ClientConnectionInc(user)

	var m dto.Metric
	g, _ := clientConnectionNum.GetMetricWithLabelValues(user)
	g.Write(&m)
	v := m.GetGauge().GetValue()

	assert.EqualValues(t, 1, v)

	ClientConnectionDec(user)

	g, _ = clientConnectionNum.GetMetricWithLabelValues(user)
	g.Write(&m)
	v = m.GetGauge().GetValue()

	assert.EqualValues(t, 0, v)
}

func TestBackendConnectionIncDec(t *testing.T) {
	address := "192.168.0.2:3306"
	BackendConnectionInc(address)

	var m dto.Metric
	g, _ := backendConnectionNum.GetMetricWithLabelValues(address)
	g.Write(&m)
	v := m.GetGauge().GetValue()

	assert.EqualValues(t, 1, v)

	BackendConnectionDec(address)

	g, _ = backendConnectionNum.GetMetricWithLabelValues(address)
	g.Write(&m)
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
	g.Write(&m)
	v := m.GetCounter().GetValue()
	assert.EqualValues(t, 2, v)

	command = "Unsupport"
	result = "Error"
	QueryTotalCounterInc(command, result)

	g, _ = queryTotalCounter.GetMetricWithLabelValues(command, result)
	g.Write(&m)
	v = m.GetCounter().GetValue()

	assert.EqualValues(t, 1, v)
}
