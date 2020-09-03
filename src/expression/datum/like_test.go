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
			right: NewDString("%", 10, false),
			res:   NewDNull(true),
		},
		{
			left:  NewDString("", 10, false),
			right: NewDString("", 10, false),
			res:   NewDInt(1, false),
		},
		{
			left:  NewDString(" ", 10, false),
			right: NewDString("", 10, false),
			res:   NewDInt(0, false),
		},
		{
			left:  NewDString("byz", 10, false),
			right: NewDString("%", 10, false),
			res:   NewDInt(1, false),
		},
		{
			left:  NewDString("byz", 10, false),
			right: NewDString("BYZ", 10, false),
			res:   NewDInt(1, false),
		},
		{
			left:  NewDString("byz", 10, false),
			right: NewDString("B%Z", 10, false),
			res:   NewDInt(1, false),
		},
		{
			left:  NewDString("byz", 10, true),
			right: NewDString("BYZ", 10, false),
			res:   NewDInt(0, false),
		},
		{
			left:  NewDString("byz", 10, false),
			right: NewDString("_%%", 10, false),
			res:   NewDInt(1, false),
		},
		{
			left:   NewDString("byz", 10, false),
			right:  NewDString("_%", 10, false),
			escape: NewDString("_", 10, false),
			res:    NewDInt(0, false),
		},
		{
			left:   NewDString("byz", 10, false),
			right:  NewDString("_b%", 10, false),
			escape: NewDString("_", 10, false),
			not:    true,
			res:    NewDInt(0, false),
		},
		{
			left:  NewDString("byz", 10, false),
			right: NewDString("\b%", 10, false),
			res:   NewDInt(0, false),
		},
		{
			left:   NewDString("byz", 10, false),
			right:  NewDString("by%_", 10, false),
			escape: NewDString("_", 10, false),
			res:    NewDInt(0, false),
		},
		{
			left:  NewDString("byz", 10, true),
			right: NewDString("by%_", 10, false),
			res:   NewDInt(1, false),
		},
		{
			left:  NewDString("byz", 10, false),
			right: NewDString("byz_", 10, false),
			res:   NewDInt(0, false),
		},
		{
			left:   NewDString("byz", 10, false),
			right:  NewDString("by%_", 10, false),
			escape: NewDString("__", 10, false),
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
