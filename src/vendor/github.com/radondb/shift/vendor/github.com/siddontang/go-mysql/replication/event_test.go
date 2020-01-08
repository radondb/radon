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

func TestMariadbGTIDEvent(t *testing.T) {
	data := []byte{
		1, 2, 3, 4, 5, 6, 7, 8, // SequenceNumber
		0x2a, 1, 0x3b, 4, // DomainID
		0xff,                                           // Flags
		0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, // commitID
	}
	ev := MariadbGTIDEvent{}
	err := ev.Decode(data)

	assert.Nil(t, err)

	assert.Equal(t, uint64(0x0807060504030201), ev.GTID.SequenceNumber)
	assert.Equal(t, uint32(0x043b012a), ev.GTID.DomainID)
	assert.Equal(t, byte(0xff), ev.Flags)
	assert.True(t, ev.IsDDL())
	assert.True(t, ev.IsStandalone())
	assert.True(t, ev.IsGroupCommit())
	assert.Equal(t, uint64(0x1716151413121110), ev.CommitID)
}
