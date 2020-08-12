package datum

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompareLike(t *testing.T) {
	tcases := []struct {
		left       Datum
		right      string
		escape     byte
		ignoreCase bool
		res        bool
		size       int
	}{
		{
			left:       NewDString("", 10),
			right:      "",
			escape:     byte('\\'),
			ignoreCase: true,
			res:        true,
			size:       0,
		},
		{
			left:       NewDString(" ", 10),
			right:      "",
			escape:     byte('\\'),
			ignoreCase: true,
			res:        false,
			size:       0,
		},
		{
			left:       NewDString("byz", 10),
			right:      "%",
			escape:     byte('\\'),
			ignoreCase: true,
			res:        true,
			size:       0,
		},
		{
			left:       NewDString("byz", 10),
			right:      "BYZ",
			escape:     byte('\\'),
			ignoreCase: true,
			res:        true,
			size:       6,
		},
		{
			left:       NewDString("byz", 10),
			right:      "BYZ",
			escape:     byte('\\'),
			ignoreCase: false,
			res:        false,
			size:       6,
		},
		{
			left:       NewDString("byz", 10),
			right:      "_%",
			escape:     byte('\\'),
			ignoreCase: true,
			res:        true,
			size:       4,
		},
		{
			left:       NewDString("byz", 10),
			right:      "_%",
			escape:     byte('_'),
			ignoreCase: true,
			res:        false,
			size:       4,
		},
	}
	for _, tcase := range tcases {
		cmp := NewCmpLike(tcase.right, tcase.escape, tcase.ignoreCase)
		res := cmp.Compare(tcase.left)
		assert.Equal(t, tcase.res, res)
		assert.Equal(t, tcase.size, cmp.Size())
	}
}
