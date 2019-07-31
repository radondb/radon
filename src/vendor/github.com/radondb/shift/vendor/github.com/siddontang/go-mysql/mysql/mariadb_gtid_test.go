package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type mariaDBTestSuite struct {
}

var t = &mariaDBTestSuite{}

// no need now
/*
func (t *mariaDBTestSuite) SetUpSuite(c *check.C) {

}

func (t *mariaDBTestSuite) TearDownSuite(c *check.C) {

}
*/

func TestParseMariaDBGTID(t *testing.T) {
	cases := []struct {
		gtidStr   string
		hashError bool
	}{
		{"0-1-1", false},
		{"", false},
		{"0-1-1-1", true},
		{"1", true},
		{"0-1-seq", true},
	}

	for _, cs := range cases {
		gtid, err := ParseMariadbGTID(cs.gtidStr)
		if cs.hashError {
			assert.NotNil(t, err)
		} else {
			assert.Nil(t, err)
			assert.EqualValues(t, cs.gtidStr, gtid.String())
		}
	}
}

func TestMariaDBGTIDConatin(t *testing.T) {
	cases := []struct {
		originGTIDStr, otherGTIDStr string
		contain                     bool
	}{
		{"0-1-1", "0-1-2", false},
		{"0-1-1", "", true},
		{"2-1-1", "1-1-1", false},
		{"1-2-1", "1-1-1", true},
		{"1-2-2", "1-1-1", true},
	}

	for _, cs := range cases {
		originGTID, err := ParseMariadbGTID(cs.originGTIDStr)
		assert.Nil(t, err)
		otherGTID, err := ParseMariadbGTID(cs.otherGTIDStr)
		assert.Nil(t, err)

		assert.Equal(t, cs.contain, originGTID.Contain(otherGTID))
	}
}

func TestMariaDBGTIDClone(t *testing.T) {
	gtid, err := ParseMariadbGTID("1-1-1")
	assert.Nil(t, err)

	clone := gtid.Clone()
	assert.EqualValues(t, gtid, clone)
}

func TestMariaDBForward(t *testing.T) {
	cases := []struct {
		currentGTIDStr, newerGTIDStr string
		hashError                    bool
	}{
		{"0-1-1", "0-1-2", false},
		{"0-1-1", "", false},
		{"2-1-1", "1-1-1", true},
		{"1-2-1", "1-1-1", false},
		{"1-2-2", "1-1-1", false},
	}

	for _, cs := range cases {
		currentGTID, err := ParseMariadbGTID(cs.currentGTIDStr)
		assert.Nil(t, err)
		newerGTID, err := ParseMariadbGTID(cs.newerGTIDStr)
		assert.Nil(t, err)

		err = currentGTID.forward(newerGTID)
		if cs.hashError {
			assert.NotNil(t, err)
			assert.Equal(t, currentGTID.String(), cs.currentGTIDStr)
		} else {
			assert.Nil(t, err)
			assert.Equal(t, currentGTID.String(), cs.newerGTIDStr)
		}
	}
}

func TestParseMariaDBGTIDSet(t *testing.T) {
	cases := []struct {
		gtidStr     string
		subGTIDs    map[uint32]string //domain ID => gtid string
		expectedStr []string          // test String()
		hasError    bool
	}{
		{"0-1-1", map[uint32]string{0: "0-1-1"}, []string{"0-1-1"}, false},
		{"", nil, []string{""}, false},
		{"0-1-1,1-2-3", map[uint32]string{0: "0-1-1", 1: "1-2-3"}, []string{"0-1-1,1-2-3", "1-2-3,0-1-1"}, false},
		{"0-1--1", nil, nil, true},
	}

	for _, cs := range cases {
		gtidSet, err := ParseMariadbGTIDSet(cs.gtidStr)
		if cs.hasError {
			assert.NotNil(t, err)
		} else {
			assert.Nil(t, err)
			mariadbGTIDSet, ok := gtidSet.(*MariadbGTIDSet)
			assert.True(t, ok)

			// check sub gtid
			assert.Len(t, mariadbGTIDSet.Sets, len(cs.subGTIDs))
			for domainID, gtid := range mariadbGTIDSet.Sets {
				assert.NotNil(t, mariadbGTIDSet.Sets[domainID])
				assert.Equal(t, gtid.String(), cs.subGTIDs[domainID])
			}

			// check String() function
			inExpectedResult := false
			actualStr := mariadbGTIDSet.String()
			for _, str := range cs.expectedStr {
				if str == actualStr {
					inExpectedResult = true
					break
				}
			}
			assert.True(t, inExpectedResult)
		}
	}
}

func TestMariaDBGTIDSetUpdate(t *testing.T) {
	cases := []struct {
		isNilGTID bool
		gtidStr   string
		subGTIDs  map[uint32]string
	}{
		{true, "", map[uint32]string{1: "1-1-1", 2: "2-2-2"}},
		{false, "1-2-2", map[uint32]string{1: "1-2-2", 2: "2-2-2"}},
		{false, "1-2-1", map[uint32]string{1: "1-2-1", 2: "2-2-2"}},
		{false, "3-2-1", map[uint32]string{1: "1-1-1", 2: "2-2-2", 3: "3-2-1"}},
	}

	for _, cs := range cases {
		gtidSet, err := ParseMariadbGTIDSet("1-1-1,2-2-2")
		assert.Nil(t, err)
		mariadbGTIDSet, ok := gtidSet.(*MariadbGTIDSet)
		assert.True(t, ok)

		if cs.isNilGTID {
			assert.Nil(t, mariadbGTIDSet.AddSet(nil))
		} else {
			err := gtidSet.Update(cs.gtidStr)
			assert.Nil(t, err)
		}
		// check sub gtid
		assert.Len(t, mariadbGTIDSet.Sets, len(cs.subGTIDs))
		for domainID, gtid := range mariadbGTIDSet.Sets {
			assert.NotNil(t, mariadbGTIDSet.Sets[domainID])
			assert.Equal(t, gtid.String(), cs.subGTIDs[domainID])
		}
	}
}

func TestMariaDBGTIDSetEqual(t *testing.T) {
	cases := []struct {
		originGTIDStr, otherGTIDStr string
		equals                      bool
	}{
		{"", "", true},
		{"1-1-1", "1-1-1,2-2-2", false},
		{"1-1-1,2-2-2", "1-1-1", false},
		{"1-1-1,2-2-2", "1-1-1,2-2-2", true},
		{"1-1-1,2-2-2", "1-1-1,2-2-3", false},
	}

	for _, cs := range cases {
		originGTID, err := ParseMariadbGTIDSet(cs.originGTIDStr)
		assert.Nil(t, err)

		otherGTID, err := ParseMariadbGTIDSet(cs.otherGTIDStr)
		assert.Nil(t, err)

		assert.Equal(t, originGTID.Equal(otherGTID), cs.equals)
	}
}

func TestMariaDBGTIDSetContain(t *testing.T) {
	cases := []struct {
		originGTIDStr, otherGTIDStr string
		contain                     bool
	}{
		{"", "", true},
		{"1-1-1", "1-1-1,2-2-2", false},
		{"1-1-1,2-2-2", "1-1-1", true},
		{"1-1-1,2-2-2", "1-1-1,2-2-2", true},
		{"1-1-1,2-2-2", "1-1-1,2-2-1", true},
		{"1-1-1,2-2-2", "1-1-1,2-2-3", false},
	}

	for _, cs := range cases {
		originGTIDSet, err := ParseMariadbGTIDSet(cs.originGTIDStr)
		assert.Nil(t, err)

		otherGTIDSet, err := ParseMariadbGTIDSet(cs.otherGTIDStr)
		assert.Nil(t, err)

		assert.Equal(t, originGTIDSet.Contain(otherGTIDSet), cs.contain)
	}
}

func TestMariaDBGTIDSetClone(t *testing.T) {
	cases := []string{"", "1-1-1", "1-1-1,2-2-2"}

	for _, str := range cases {
		gtidSet, err := ParseMariadbGTIDSet(str)
		assert.Nil(t, err)

		assert.EqualValues(t, gtidSet.Clone(), gtidSet)
	}
}
