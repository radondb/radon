/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sqltypes

import (
	"reflect"
	"strings"
	"testing"

	querypb "github.com/xelabs/go-mysqlstack/sqlparser/depends/query"
)

func TestValidateBindVarables(t *testing.T) {
	tcases := []struct {
		in  map[string]*querypb.BindVariable
		err string
	}{{
		in: map[string]*querypb.BindVariable{
			"v": {
				Type:  querypb.Type_INT64,
				Value: []byte("1"),
			},
		},
		err: "",
	}, {
		in: map[string]*querypb.BindVariable{
			"v": {
				Type:  querypb.Type_INT64,
				Value: []byte("a"),
			},
		},
		err: `v: strconv.ParseInt: parsing "a": invalid syntax`,
	}, {
		in: map[string]*querypb.BindVariable{
			"v": {
				Type: querypb.Type_TUPLE,
				Values: []*querypb.Value{{
					Type:  Int64,
					Value: []byte("a"),
				}},
			},
		},
		err: `v: strconv.ParseInt: parsing "a": invalid syntax`,
	}}
	for _, tcase := range tcases {
		err := ValidateBindVariables(tcase.in)
		if tcase.err != "" {
			if err == nil || err.Error() != tcase.err {
				t.Errorf("ValidateBindVars(%v): %v, want %s", tcase.in, err, tcase.err)
			}
			continue
		}
		if err != nil {
			t.Errorf("ValidateBindVars(%v): %v, want nil", tcase.in, err)
		}
	}
}

func TestValidateBindVariable(t *testing.T) {
	testcases := []struct {
		in  *querypb.BindVariable
		err string
	}{{
		in: &querypb.BindVariable{
			Type:  querypb.Type_INT8,
			Value: []byte("1"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_INT16,
			Value: []byte("1"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_INT24,
			Value: []byte("1"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_INT32,
			Value: []byte("1"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_INT64,
			Value: []byte("1"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_UINT8,
			Value: []byte("1"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_UINT16,
			Value: []byte("1"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_UINT24,
			Value: []byte("1"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_UINT32,
			Value: []byte("1"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_UINT64,
			Value: []byte("1"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_FLOAT32,
			Value: []byte("1.00"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_FLOAT64,
			Value: []byte("1.00"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_DECIMAL,
			Value: []byte("1.00"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_TIMESTAMP,
			Value: []byte("2012-02-24 23:19:43"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_DATE,
			Value: []byte("2012-02-24"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_TIME,
			Value: []byte("23:19:43"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_DATETIME,
			Value: []byte("2012-02-24 23:19:43"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_YEAR,
			Value: []byte("1"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_TEXT,
			Value: []byte("a"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_BLOB,
			Value: []byte("a"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_VARCHAR,
			Value: []byte("a"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_BINARY,
			Value: []byte("a"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_CHAR,
			Value: []byte("a"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_BIT,
			Value: []byte("1"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_ENUM,
			Value: []byte("a"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_SET,
			Value: []byte("a"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_VARBINARY,
			Value: []byte("a"),
		},
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_INT64,
			Value: []byte(InvalidNeg),
		},
		err: "out of range",
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_INT64,
			Value: []byte(InvalidPos),
		},
		err: "out of range",
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_UINT64,
			Value: []byte("-1"),
		},
		err: "invalid syntax",
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_UINT64,
			Value: []byte(InvalidPos),
		},
		err: "out of range",
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_FLOAT64,
			Value: []byte("a"),
		},
		err: "invalid syntax",
	}, {
		in: &querypb.BindVariable{
			Type:  querypb.Type_EXPRESSION,
			Value: []byte("a"),
		},
		err: "invalid type specified for MakeValue: EXPRESSION",
	}, {
		in: &querypb.BindVariable{
			Type: querypb.Type_TUPLE,
			Values: []*querypb.Value{{
				Type:  querypb.Type_INT64,
				Value: []byte("1"),
			}},
		},
	}, {
		in: &querypb.BindVariable{
			Type: querypb.Type_TUPLE,
		},
		err: "empty tuple is not allowed",
	}, {
		in: &querypb.BindVariable{
			Type: querypb.Type_TUPLE,
			Values: []*querypb.Value{{
				Type: querypb.Type_TUPLE,
			}},
		},
		err: "tuple not allowed inside another tuple",
	}}
	for _, tcase := range testcases {
		err := ValidateBindVariable(tcase.in)
		if tcase.err != "" {
			if err == nil || !strings.Contains(err.Error(), tcase.err) {
				t.Errorf("ValidateBindVar(%v) error: %v, must contain %v", tcase.in, err, tcase.err)
			}
			continue
		}
		if err != nil {
			t.Errorf("ValidateBindVar(%v) error: %v", tcase.in, err)
		}
	}

	// Special case: nil bind var.
	err := ValidateBindVariable(nil)
	want := "bind variable is nil"
	if err == nil || err.Error() != want {
		t.Errorf("ValidateBindVar(nil) error: %v, want %s", err, want)
	}
}

func TestBindVariableToValue(t *testing.T) {
	v, err := BindVariableToValue(Int64BindVariable(1))
	if err != nil {
		t.Error(err)
	}
	want := MakeTrusted(querypb.Type_INT64, []byte("1"))
	if !reflect.DeepEqual(v, want) {
		t.Errorf("BindVarToValue(1): %v, want %v", v, want)
	}

	v, err = BindVariableToValue(&querypb.BindVariable{Type: querypb.Type_TUPLE})
	wantErr := "cannot convert a TUPLE bind var into a value"
	if err == nil || err.Error() != wantErr {
		t.Errorf(" BindVarToValue(TUPLE): (%v, %v), want %s", v, err, wantErr)
	}
}
