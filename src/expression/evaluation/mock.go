package evaluation

import (
	"expression/datum"

	"github.com/shopspring/decimal"
	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

var (
	con1 = CONST(datum.NewDInt(3, false))
	con2 = CONST(datum.NewDString("^[A-Z0-9._%-]+@[A-Z0-9.-]+.[A-Z]{2,4}$", 10, 33))
	con3 = CONST(datum.NewDInt(-1, true))
	con4 = CONST(datum.NewDString("abc@de.fg", 10, 33))
	con5 = CONST(datum.NewDString("%", 10, 33))

	tuple = TUPLE(CONST(datum.NewDInt(3, false)), CONST(datum.NewDInt(1, true)), VAR("f"))

	fields = map[string]*querypb.Field{
		"a": {
			Name:     "a",
			Type:     querypb.Type_INT32,
			Decimals: 0,
			Flags:    128,
		},
		"b": {
			Name:     "b",
			Type:     querypb.Type_UINT64,
			Decimals: 0,
			Flags:    32,
		},
		"c": {
			Name:     "c",
			Type:     querypb.Type_CHAR,
			Decimals: 31,
			Flags:    128,
		},
		"d": {
			Name:     "d",
			Type:     querypb.Type_FLOAT64,
			Decimals: 31,
			Flags:    128,
		},
		"e": {
			Name:     "e",
			Type:     querypb.Type_DECIMAL,
			Decimals: 2,
			Flags:    128,
		},
		"f": {
			Name:     "e",
			Type:     querypb.Type_NULL_TYPE,
			Decimals: 2,
			Flags:    128,
		},
	}
	values = map[string]datum.Datum{
		"a": datum.NewDInt(1, false),
		"b": datum.NewDInt(2, true),
		"c": datum.NewDString("c", 10, 33),
		"d": datum.NewDFloat(3.20),
		"e": datum.NewDDecimal(decimal.NewFromFloat(4.30)),
		"f": datum.NewDNull(true),
	}
)
