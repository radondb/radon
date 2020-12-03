/*
 * go-mysqlstack
 * xelabs.org
 *
 * Copyright (c) XeLabs
 * GPL License
 *
 */

package common

import (
	"bytes"
	"encoding/json"
	"reflect"
	"unsafe"
)

// BytesToString casts slice to string without copy
func BytesToString(b []byte) (s string) {
	if len(b) == 0 {
		return ""
	}

	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh := reflect.StringHeader{Data: bh.Data, Len: bh.Len}

	return *(*string)(unsafe.Pointer(&sh))
}

// StringToBytes casts string to slice without copy
func StringToBytes(s string) []byte {
	if len(s) == 0 {
		return []byte{}
	}

	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := reflect.SliceHeader{Data: sh.Data, Len: sh.Len, Cap: sh.Len}

	return *(*[]byte)(unsafe.Pointer(&bh))
}

// ToJSONString format v to the JSON encoding, return a string.
func ToJSONString(v interface{}, escapeHTML bool, prefix, indent string) (string, error) {
	bf := bytes.NewBuffer([]byte{})
	jsonEncoder := json.NewEncoder(bf)
	jsonEncoder.SetEscapeHTML(escapeHTML)
	jsonEncoder.SetIndent(prefix, indent)
	if err := jsonEncoder.Encode(v); err != nil {
		return "", err
	}
	// Remove the newline added by (*Encoder).Encode.
	bf.Truncate(bf.Len() - 1)
	return bf.String(), nil
}
