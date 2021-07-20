package evaluation

import (
	"testing"

	"github.com/xelabs/go-mysqlstack/sqlparser/depends/expression/datum"

	"github.com/stretchr/testify/assert"
)

func TestConstant(t *testing.T) {
	res := con1.Result().ValStr()
	assert.Equal(t, "3", res)
}

func TestTuple(t *testing.T) {
	{
		_, err := tuple.FixField(nil)
		assert.Equal(t, "can.not.get.the.field.value:f", err.Error())
	}
	{
		tup := TUPLE(tuple)
		_, err := tup.FixField(fields)
		assert.Equal(t, "bad.argument.at.index 0: unexpected.result.type[4].in.the.argument", err.Error())
	}
	{
		_, err := tuple.FixField(fields)
		assert.Nil(t, err)

		_, err = tuple.Update(values)
		assert.Nil(t, err)

		res := tuple.Result()
		assert.Equal(t, "31NULL", res.ValStr())
	}
}

func TestVariable(t *testing.T) {
	v := VAR("a")

	_, err := v.FixField(fields)
	assert.Nil(t, err)

	_, err = v.Update(map[string]datum.Datum{
		"b": datum.NewDInt(1, false),
	})
	assert.Equal(t, "can.not.get.the.datum.value:a", err.Error())

	_, err = v.Update(values)
	assert.Nil(t, err)

	res := v.Result()
	assert.Equal(t, "1", res.ValStr())
}
