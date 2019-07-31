package websocket

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"math/rand"
	"net"
	"time"
)

//refer RFC6455

const (
	TextMessage   byte = 1
	BinaryMessage byte = 2
	CloseMessage  byte = 8
	PingMessage   byte = 9
	PongMessage   byte = 10
)

var (
	ErrControlTooLong    = errors.New("control message too long")
	ErrRSVNotSupport     = errors.New("reserved bit not support")
	ErrPayloadError      = errors.New("payload length error")
	ErrControlFragmented = errors.New("control message can not be fragmented")
	ErrNotTCPConn        = errors.New("not a tcp connection")
	ErrWriteError        = errors.New("write error")
)

type Conn struct {
	conn net.Conn

	br *bufio.Reader

	isServer bool
}

func NewConn(conn net.Conn, isServer bool) *Conn {
	c := new(Conn)

	c.conn = conn

	c.br = bufio.NewReader(conn)

	c.isServer = isServer

	return c
}

func (c *Conn) ReadMessage() (messageType byte, message []byte, err error) {
	return c.Read()
}

func (c *Conn) Read() (messageType byte, message []byte, err error) {
	buf := make([]byte, 8, 8)

	message = []byte{}

	messageType = 0

	for {
		opcode, data, err := c.readFrame(buf)

		if err != nil {
			return messageType, message, err
		}

		message = append(message, data...)

		if opcode&0x80 != 0 {
			//final
			if opcode&0x0F > 0 {
				//not continue frame
				messageType = opcode & 0x0F
			}
			return messageType, message, nil

		} else {
			if opcode&0x0F > 0 {
				//first continue frame
				messageType = opcode & 0x0F
			}
		}
	}

	return
}

func (c *Conn) Write(message []byte, binary bool) error {
	if binary {
		return c.sendFrame(BinaryMessage, message)
	} else {
		return c.sendFrame(TextMessage, message)
	}
}

func (c *Conn) WriteMessage(messageType byte, message []byte) error {
	return c.sendFrame(messageType, message)
}

//write utf-8 text message
func (c *Conn) WriteString(message []byte) error {
	return c.Write(message, false)
}

//write binary message
func (c *Conn) WriteBinary(message []byte) error {
	return c.Write(message, true)
}

func (c *Conn) Ping(message []byte) error {
	return c.sendFrame(PingMessage, message)
}

func (c *Conn) Pong(message []byte) error {
	return c.sendFrame(PongMessage, message)
}

//close socket, not send websocket close message
func (c *Conn) Close() error {
	return c.conn.Close()
}

func (c *Conn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *Conn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}

func (c *Conn) SetReadBuffer(bytes int) error {
	if tcpConn, ok := c.conn.(*net.TCPConn); ok {
		return tcpConn.SetReadBuffer(bytes)
	} else {
		return ErrNotTCPConn
	}
}

func (c *Conn) SetWriteBuffer(bytes int) error {
	if tcpConn, ok := c.conn.(*net.TCPConn); ok {
		return tcpConn.SetWriteBuffer(bytes)
	} else {
		return ErrNotTCPConn
	}
}

func (c *Conn) readPayloadLen(length byte, buf []byte) (payloadLen uint64, err error) {
	if length < 126 {
		payloadLen = uint64(length)
	} else if length == 126 {
		err = c.read(buf[:2])
		if err != nil {
			return
		}
		payloadLen = uint64(binary.BigEndian.Uint16(buf[:2]))
	} else if length == 127 {
		err = c.read(buf[:8])
		if err != nil {
			return
		}
		payloadLen = uint64(binary.BigEndian.Uint16(buf[:8]))
	}

	return
}

func (c *Conn) readFrame(buf []byte) (opcode byte, messsage []byte, err error) {
	//minimum head may 2 byte

	err = c.read(buf[:2])
	if err != nil {
		return
	}

	opcode = buf[0]

	if opcode&0x70 > 0 {
		err = ErrRSVNotSupport
		return
	}

	//isMasking := (0x80 & buf[1]) > 0
	isMasking := (0x80 & buf[1]) > 0

	var payloadLen uint64
	payloadLen, err = c.readPayloadLen(buf[1]&0x7F, buf)
	if err != nil {
		return
	}

	if opcode&0x08 > 0 && payloadLen > 125 {
		err = ErrControlTooLong
		return
	}

	var masking []byte

	if isMasking {
		err = c.read(buf[:4])
		if err != nil {
			return
		}

		masking = buf[:4]
	}

	messsage = make([]byte, payloadLen)
	err = c.read(messsage)

	if err != nil {
		return
	}

	if isMasking {
		//maskingKey := c.newMaskingKey()
		c.maskingData(messsage, masking)
	}

	return
}

func (c *Conn) sendFrame(opcode byte, message []byte) error {
	//max frame header may 14 length
	buf := make([]byte, 0, len(message)+14)
	//here we don not support continue frame, all are final
	opcode |= 0x80

	if opcode&0x08 > 0 && len(message) >= 126 {
		return ErrControlTooLong
	}

	buf = append(buf, opcode)

	//no mask, because chrome may not support
	var mask byte = 0x00

	if !c.isServer {
		//for client, we will mask data
		mask = 0x80
	}

	payloadLen := len(message)

	if payloadLen < 126 {
		buf = append(buf, mask|byte(payloadLen))
	} else if payloadLen <= 0xFFFF {
		buf = append(buf, mask|byte(126), 0, 0)

		binary.BigEndian.PutUint16(buf[len(buf)-2:], uint16(payloadLen))
	} else {
		buf = append(buf, mask|byte(127), 0, 0, 0, 0, 0, 0, 0, 0)

		binary.BigEndian.PutUint64(buf[len(buf)-8:], uint64(payloadLen))
	}

	if !c.isServer {
		maskingKey := c.newMaskingKey()
		buf = append(buf, maskingKey...)

		pos := len(buf)
		buf = append(buf, message...)

		c.maskingData(buf[pos:], maskingKey)

	} else {
		buf = append(buf, message...)
	}

	tmpBuf := buf
	for i := 0; i < 3; i++ {
		n, err := c.conn.Write(tmpBuf)
		if err != nil {
			return err
		}
		if n == len(tmpBuf) {
			return nil
		} else {
			tmpBuf = tmpBuf[n:]
		}
	}
	return ErrWriteError
}

func (c *Conn) read(buf []byte) error {
	var err error
	for len(buf) > 0 && err == nil {
		var nn int
		nn, err = c.br.Read(buf)
		buf = buf[nn:]
	}
	if err == io.EOF {
		if len(buf) == 0 {
			err = nil
		} else {
			err = io.ErrUnexpectedEOF
		}
	}
	return err
}

func (c *Conn) maskingData(data []byte, maskingKey []byte) {
	for i := range data {
		data[i] ^= maskingKey[i%4]
	}
}

func (c *Conn) newMaskingKey() []byte {
	n := rand.Uint32()
	return []byte{byte(n), byte(n >> 8), byte(n >> 16), byte(n >> 32)}
}
