package datum

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRegexp(t *testing.T) {
	tcases := []struct {
		left  Datum
		right Datum
		not   bool
		res   Datum
	}{
		{
			left:  NewDNull(true),
			right: NewDString("^[A-Z0-9._%-]+@[A-Z0-9.-]+.[A-Z]{2,4}$", 10, 33),
			not:   false,
			res:   NewDNull(true),
		},
		{
			left:  NewDString("abc@de", 10, 33),
			right: NewDString("^[A-Z0-9._%-]+@[A-Z0-9.-]+.[A-Z]{2,4}$", 10, 33),
			not:   true,
			res:   NewDInt(1, false),
		},
		{
			left:  NewDString("abc@de.fg", 10, 33),
			right: NewDString("^[A-Z0-9._%-]+@[A-Z0-9.-]+.[A-Z]{2,4}$", 10, 33),
			not:   false,
			res:   NewDInt(1, false),
		},
		{
			left:  NewDString("abc@de.fg", 10, 63),
			right: NewDString("^[A-Z0-9._%-]+@[A-Z0-9.-]+.[A-Z]{2,4}$", 10, 33),
			not:   false,
			res:   NewDInt(0, false),
		},
	}

	for _, tcase := range tcases {
		res := Regexp(tcase.left, tcase.right, tcase.not)
		assert.Equal(t, tcase.res, res)
	}
}
