package rpc

import (
	"container/list"
	"fmt"
	"reflect"
	"sync"
)

type Client struct {
	sync.Mutex

	network string
	addr    string

	maxIdleConns int

	conns *list.List
}

func NewClient(network, addr string, maxIdleConns int) *Client {
	RegisterType(RpcError{})

	c := new(Client)
	c.network = network
	c.addr = addr

	c.maxIdleConns = maxIdleConns

	c.conns = list.New()

	return c
}

func (c *Client) Close() error {
	c.Lock()

	for {
		if c.conns.Len() > 0 {
			v := c.conns.Front()

			co := v.Value.(*conn)
			co.Close()
			c.conns.Remove(v)
		} else {
			break
		}
	}

	c.Unlock()
	return nil
}

func (c *Client) MakeRpc(rpcName string, fptr interface{}) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("make rpc error")
		}
	}()

	fn := reflect.ValueOf(fptr).Elem()

	nOut := fn.Type().NumOut()
	if nOut == 0 || fn.Type().Out(nOut-1).Kind() != reflect.Interface {
		err = fmt.Errorf("%s return final output param must be error interface", rpcName)
		return
	}

	_, b := fn.Type().Out(nOut - 1).MethodByName("Error")
	if !b {
		err = fmt.Errorf("%s return final output param must be error interface", rpcName)
		return
	}

	f := func(in []reflect.Value) []reflect.Value {
		return c.call(fn, rpcName, in)
	}

	v := reflect.MakeFunc(fn.Type(), f)
	fn.Set(v)

	return
}

func (c *Client) call(fn reflect.Value, name string, in []reflect.Value) []reflect.Value {
	inArgs := make([]interface{}, len(in))
	for i := 0; i < len(in); i++ {
		inArgs[i] = in[i].Interface()
	}

	data, err := encodeData(name, inArgs)
	if err != nil {
		return c.returnCallError(fn, err)
	}

	var co *conn
	var buf []byte
	for i := 0; i < 3; i++ {
		if co, err = c.popConn(); err != nil {
			continue
		}

		buf, err = co.Call(data)
		if err == nil {
			c.pushConn(co)
			break
		} else {
			co.Close()
		}
	}

	if err != nil {
		return c.returnCallError(fn, err)
	}

	n, out, e := decodeData(buf)
	if e != nil {
		return c.returnCallError(fn, e)
	}

	if n != name {
		return c.returnCallError(fn, fmt.Errorf("rpc name %s != %s", n, name))
	}

	last := out[len(out)-1]
	if last != nil {
		if err, ok := last.(error); ok {
			return c.returnCallError(fn, err)
		} else {
			return c.returnCallError(fn, fmt.Errorf("rpc final return type %T must be error", last))
		}
	}

	outValues := make([]reflect.Value, len(out))
	for i := 0; i < len(out); i++ {
		if out[i] == nil {
			outValues[i] = reflect.Zero(fn.Type().Out(i))
		} else {
			outValues[i] = reflect.ValueOf(out[i])
		}
	}

	return outValues
}

func (c *Client) returnCallError(fn reflect.Value, err error) []reflect.Value {
	nOut := fn.Type().NumOut()
	out := make([]reflect.Value, nOut)
	for i := 0; i < nOut-1; i++ {
		out[i] = reflect.Zero(fn.Type().Out(i))
	}

	out[nOut-1] = reflect.ValueOf(&err).Elem()
	return out
}

func (c *Client) popConn() (*conn, error) {
	c.Lock()
	if c.conns.Len() > 0 {
		v := c.conns.Front()
		c.conns.Remove(v)
		c.Unlock()

		return v.Value.(*conn), nil
	}
	c.Unlock()
	return newConn(c.network, c.addr)
}

func (c *Client) pushConn(co *conn) error {
	c.Lock()
	if c.conns.Len() >= c.maxIdleConns {
		c.Unlock()
		co.Close()
		return nil
	} else {
		c.conns.PushBack(co)
	}
	c.Unlock()
	return nil
}
