package datum

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLike(t *testing.T) {
	tcases := []struct {
		left   Datum
		right  Datum
		escape Datum
		not    bool
		res    Datum
		err    string
	}{
		{
			left:  NewDNull(true),
			right: NewDString("%", 10, 33),
			res:   NewDNull(true),
		},
		{
			left:  NewDString("", 10, 33),
			right: NewDString("", 10, 33),
			res:   NewDInt(1, false),
		},
		{
			left:  NewDString(" ", 10, 33),
			right: NewDString("", 10, 33),
			res:   NewDInt(0, false),
		},
		{
			left:  NewDString("byz", 10, 33),
			right: NewDString("%", 10, 33),
			res:   NewDInt(1, false),
		},
		{
			left:  NewDString("byz", 10, 33),
			right: NewDString("BYZ", 10, 33),
			res:   NewDInt(1, false),
		},
		{
			left:  NewDString("byz", 10, 33),
			right: NewDString("B%Z", 10, 33),
			res:   NewDInt(1, false),
		},
		{
			left:  NewDString("byz", 10, 63),
			right: NewDString("BYZ", 10, 33),
			res:   NewDInt(0, false),
		},
		{
			left:  NewDString("byz", 10, 33),
			right: NewDString("_%%", 10, 33),
			res:   NewDInt(1, false),
		},
		{
			left:   NewDString("byz", 10, 33),
			right:  NewDString("_%", 10, 33),
			escape: NewDString("_", 10, 33),
			res:    NewDInt(0, false),
		},
		{
			left:   NewDString("byz", 10, 33),
			right:  NewDString("_b%", 10, 33),
			escape: NewDString("_", 10, 33),
			not:    true,
			res:    NewDInt(0, false),
		},
		{
			left:  NewDString("byz", 10, 33),
			right: NewDString("\b%", 10, 33),
			res:   NewDInt(0, false),
		},
		{
			left:   NewDString("byz", 10, 33),
			right:  NewDString("by%_", 10, 33),
			escape: NewDString("_", 10, 33),
			res:    NewDInt(0, false),
		},
		{
			left:  NewDString("byz", 10, 63),
			right: NewDString("by%_", 10, 33),
			res:   NewDInt(1, false),
		},
		{
			left:  NewDString("byz", 10, 33),
			right: NewDString("byz_", 10, 33),
			res:   NewDInt(0, false),
		},
		{
			left:   NewDString("byz", 10, 33),
			right:  NewDString("by%_", 10, 33),
			escape: NewDString("__", 10, 33),
			err:    "Incorrect.arguments.to.ESCAPE",
		},
	}
	for _, tcase := range tcases {
		res, err := Like(tcase.left, tcase.right, tcase.escape, tcase.not)
		if err != nil {
			assert.Equal(t, tcase.err, err.Error())
		} else {
			assert.Equal(t, tcase.res, res)
		}
	}
}
