package replication

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMariadbGTIDListEvent(t *testing.T) {
	// single GTID, 1-2-3
	data := []byte{1, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0}
	ev := MariadbGTIDListEvent{}
	err := ev.Decode(data)
	assert.Nil(t, err)
	assert.Len(t, ev.GTIDs, 1)
	assert.Equal(t, uint32(1), ev.GTIDs[0].DomainID)
	assert.Equal(t, uint32(2), ev.GTIDs[0].ServerID)
	assert.Equal(t, uint64(3), ev.GTIDs[0].SequenceNumber)

	// multi GTIDs, 1-2-3,4-5-6,7-8-9
	data = []byte{3, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 5, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 8, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0}
	ev = MariadbGTIDListEvent{}
	err = ev.Decode(data)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(ev.GTIDs))
	for i := 0; i < 3; i++ {
		assert.Equal(t, uint32(1+3*i), ev.GTIDs[i].DomainID)
		assert.Equal(t, uint32(2+3*i), ev.GTIDs[i].ServerID)
		assert.Equal(t, uint64(3+3*i), ev.GTIDs[i].SequenceNumber)
	}
}
