package rpc

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type conn struct {
	co net.Conn
}

func newConn(network, addr string) (*conn, error) {
	c, err := net.Dial(network, addr)
	if err != nil {
		return nil, err
	}

	co := new(conn)
	co.co = c
	return co, nil
}

func (c *conn) Close() error {
	return c.co.Close()
}

func (c *conn) Call(data []byte) ([]byte, error) {
	if err := c.WriteMessage(data); err != nil {
		return nil, err
	}

	if buf, err := c.ReadMessage(); err != nil {
		return nil, err
	} else {
		return buf, nil
	}
}

func (c *conn) WriteMessage(data []byte) error {
	buf := make([]byte, 4+len(data))

	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(data)))

	copy(buf[4:], data)

	n, err := c.co.Write(buf)
	if err != nil {
		c.Close()
		return err
	} else if n != len(buf) {
		c.Close()
		return fmt.Errorf("write %d less than %d", n, len(buf))
	}
	return nil
}

func (c *conn) ReadMessage() ([]byte, error) {
	l := make([]byte, 4)

	_, err := io.ReadFull(c.co, l)
	if err != nil {
		c.Close()
		return nil, err
	}

	length := binary.LittleEndian.Uint32(l)

	data := make([]byte, length)
	_, err = io.ReadFull(c.co, data)
	if err != nil {
		c.Close()
		return nil, err
	} else {
		return data, nil
	}
}
