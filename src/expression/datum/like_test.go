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
			right: NewDString("%", 10),
			res:   NewDNull(true),
		},
		{
			left:  NewDString("", 10),
			right: NewDString("", 10),
			res:   NewDInt(1, false),
		},
		{
			left:  NewDString(" ", 10),
			right: NewDString("", 10),
			res:   NewDInt(0, false),
		},
		{
			left:  NewDString("byz", 10),
			right: NewDString("%", 10),
			res:   NewDInt(1, false),
		},
		{
			left:  NewDString("byz", 10),
			right: NewDString("BYZ", 10),
			res:   NewDInt(1, false),
		},
		{
			left:  NewDString("byz", 10),
			right: NewDString("B%Z", 10),
			res:   NewDInt(1, false),
		},
		{
			left:  &DString{"byz", 10, false},
			right: NewDString("BYZ", 10),
			res:   NewDInt(0, false),
		},
		{
			left:  NewDString("byz", 10),
			right: NewDString("_%%", 10),
			res:   NewDInt(1, false),
		},
		{
			left:   NewDString("byz", 10),
			right:  NewDString("_%", 10),
			escape: NewDString("_", 10),
			res:    NewDInt(0, false),
		},
		{
			left:   NewDString("byz", 10),
			right:  NewDString("_b%", 10),
			escape: NewDString("_", 10),
			not:    true,
			res:    NewDInt(0, false),
		},
		{
			left:  NewDString("byz", 10),
			right: NewDString("\b%", 10),
			res:   NewDInt(0, false),
		},
		{
			left:   NewDString("byz", 10),
			right:  NewDString("by%_", 10),
			escape: NewDString("_", 10),
			res:    NewDInt(0, false),
		},
		{
			left:  &DString{"byz", 10, false},
			right: NewDString("by%_", 10),
			res:   NewDInt(1, false),
		},
		{
			left:  NewDString("byz", 10),
			right: NewDString("byz_", 10),
			res:   NewDInt(0, false),
		},
		{
			left:   NewDString("byz", 10),
			right:  NewDString("by%_", 10),
			escape: NewDString("__", 10),
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
