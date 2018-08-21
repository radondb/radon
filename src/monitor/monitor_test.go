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
