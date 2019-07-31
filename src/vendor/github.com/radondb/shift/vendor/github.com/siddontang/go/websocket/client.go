package websocket

import (
	"bytes"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
)

var (
	ErrBadHandshake = errors.New("bad handshake")
)

func NewClient(netConn net.Conn, u *url.URL, requestHeader http.Header) (c *Conn, response *http.Response, err error) {
	key, err := calcKey()
	if err != nil {
		return nil, nil, err
	}
	acceptKey := calcAcceptKey(key)

	c = NewConn(netConn, false)

	buf := bytes.NewBufferString("GET ")
	buf.WriteString(u.RequestURI())
	buf.WriteString(" HTTP/1.1\r\nHost: ")
	buf.WriteString(u.Host)
	buf.WriteString("\r\nUpgrade: websocket\r\nConnection: upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: ")
	buf.WriteString(key)
	buf.WriteString("\r\n")

	for k, vs := range requestHeader {
		for _, v := range vs {
			buf.WriteString(k)
			buf.WriteString(": ")
			buf.WriteString(v)
			buf.WriteString("\r\n")
		}
	}

	buf.WriteString("\r\n")
	p := buf.Bytes()
	if _, err := netConn.Write(p); err != nil {
		return nil, nil, err
	}

	resp, err := http.ReadResponse(c.br, &http.Request{Method: "GET", URL: u})
	if err != nil {
		return nil, nil, err
	}

	if resp.StatusCode != 101 ||
		!strings.EqualFold(resp.Header.Get("Upgrade"), "websocket") ||
		!strings.EqualFold(resp.Header.Get("Connection"), "upgrade") ||
		resp.Header.Get("Sec-Websocket-Accept") != acceptKey {
		return nil, resp, ErrBadHandshake
	}
	return c, resp, nil
}
