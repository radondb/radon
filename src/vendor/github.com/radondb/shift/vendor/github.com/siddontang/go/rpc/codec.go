package rpc

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

type RpcError struct {
	Message string
}

func (r RpcError) Error() string {
	return r.Message
}

func RegisterType(value interface{}) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("register error")
		}
	}()
	gob.Register(value)
	return
}

type rpcData struct {
	Name string
	Args []interface{}
}

func encodeData(name string, args []interface{}) ([]byte, error) {
	d := rpcData{}
	d.Name = name

	d.Args = args

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	if err := enc.Encode(d); err != nil {
		return nil, err
	} else {
		return buf.Bytes(), nil
	}
}

func decodeData(data []byte) (name string, args []interface{}, err error) {
	var d rpcData

	var buf = bytes.NewBuffer(data)

	dec := gob.NewDecoder(buf)

	if err = dec.Decode(&d); err != nil {
		return
	}

	name = d.Name
	args = d.Args

	return
}
