package websocket

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"net/http"
	"strings"
)

var (
	ErrInvalidMethod     = errors.New("Only GET Supported")
	ErrInvalidVersion    = errors.New("Sec-Websocket-Version: 13")
	ErrInvalidUpgrade    = errors.New("Can \"Upgrade\" only to \"WebSocket\"")
	ErrInvalidConnection = errors.New("\"Connection\" must be \"Upgrade\"")
	ErrMissingKey        = errors.New("Missing Key")
	ErrHijacker          = errors.New("Not implement http.Hijacker")
	ErrNoEmptyConn       = errors.New("Conn ReadBuf must be empty")
)

func Upgrade(w http.ResponseWriter, r *http.Request, responseHeader http.Header) (*Conn, error) {
	if r.Method != "GET" {
		return nil, ErrInvalidMethod
	}

	if r.Header.Get("Sec-Websocket-Version") != "13" {
		return nil, ErrInvalidVersion
	}

	if strings.ToLower(r.Header.Get("Upgrade")) != "websocket" {
		return nil, ErrInvalidUpgrade
	}

	if strings.ToLower(r.Header.Get("Connection")) != "upgrade" {
		return nil, ErrInvalidConnection
	}

	var acceptKey string

	if key := r.Header.Get("Sec-Websocket-key"); len(key) == 0 {
		return nil, ErrMissingKey
	} else {
		acceptKey = calcAcceptKey(key)
	}

	var (
		netConn net.Conn
		br      *bufio.Reader
		err     error
	)

	h, ok := w.(http.Hijacker)
	if !ok {
		return nil, ErrHijacker
	}

	var rw *bufio.ReadWriter
	netConn, rw, err = h.Hijack()
	br = rw.Reader

	if br.Buffered() > 0 {
		netConn.Close()
		return nil, ErrNoEmptyConn
	}

	c := NewConn(netConn, true)

	buf := bytes.NewBufferString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ")

	buf.WriteString(acceptKey)
	buf.WriteString("\r\n")

	subProtol := selectSubProtocol(r)
	if len(subProtol) > 0 {
		buf.WriteString("Sec-Websocket-Protocol: ")
		buf.WriteString(subProtol)
		buf.WriteString("\r\n")
	}

	for k, vs := range responseHeader {
		for _, v := range vs {
			buf.WriteString(k)
			buf.WriteString(": ")
			buf.WriteString(v)
			buf.WriteString("\r\n")
		}
	}
	buf.WriteString("\r\n")

	if _, err = netConn.Write(buf.Bytes()); err != nil {
		netConn.Close()
		return nil, err
	}

	return c, nil
}

func selectSubProtocol(r *http.Request) string {
	h := r.Header.Get("Sec-Websocket-Protocol")
	if len(h) == 0 {
		return ""
	}
	return strings.Split(h, ",")[0]
}
