package websocket

import (
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func TestWSPing(t *testing.T) {
	http.HandleFunc("/test/ping", func(w http.ResponseWriter, r *http.Request) {
		conn, err := Upgrade(w, r, nil)
		if err != nil {
			t.Fatal(err.Error())
		}
		//conn := NewConn(c, true)
		conn.Read()
		conn.Pong([]byte{})
		conn.Ping([]byte{})
		msgType, _, _ := conn.Read()
		println(msgType)
	})

	go http.ListenAndServe(":65500", nil)
	time.Sleep(time.Second * 1)

	conn, err := net.Dial("tcp", "127.0.0.1:65500")

	if err != nil {
		t.Fatal(err.Error())
	}
	ws, _, err := NewClient(conn, &url.URL{Host: "127.0.0.1:65500", Path: "/test/ping"}, nil)

	if err != nil {
		t.Fatal(err.Error())
	}
	ws.Ping([]byte{})

	msgType, _, _ := ws.Read()
	if msgType != PongMessage {
		t.Fatal("invalid msg type", msgType)
	}

	msgType, _, _ = ws.Read()
	if msgType != PingMessage {
		t.Fatal("invalid msg type", msgType)
	}
	ws.Pong([]byte{})
	time.Sleep(time.Second * 1)
}
