package websocket

import (
	"net"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestWSServer(t *testing.T) {
	http.HandleFunc("/test/server", func(w http.ResponseWriter, r *http.Request) {
		conn, err := Upgrade(w, r, nil)

		if err != nil {
			t.Fatal(err.Error())
		}
		//err = conn.SetReadBuffer(1024 * 1024 * 4)
		//if err != nil {
		//	println(err.Error())
		//}
		//err = conn.SetWriteBuffer(1024 * 1024 * 4)

		//if err != nil {
		//	println(err.Error())
		//}

		msgType, msg, err := conn.Read()
		conn.Write(msg, false)

		if err != nil {
			t.Fatal(err.Error())
		}

		if msgType != TextMessage {
			t.Fatal("wrong msg type", msgType)
		}

		msgType, msg, err = conn.ReadMessage()
		if err != nil {
			t.Fatal(err.Error())
		}

		if msgType != PingMessage {
			t.Fatal("wrong msg type", msgType)
		}

		err = conn.Pong([]byte("abc"))

		if err != nil {
			t.Fatal(err.Error())
		}

	})

	go http.ListenAndServe(":65500", nil)
	time.Sleep(time.Second * 1)

	conn, err := net.Dial("tcp", "127.0.0.1:65500")

	if err != nil {
		t.Fatal(err.Error())
	}
	ws, _, err := websocket.NewClient(conn, &url.URL{Scheme: "ws", Host: "127.0.0.1:65500", Path: "/test/server"}, nil, 1024, 1024)

	ws.SetPongHandler(func(string) error {
		println("pong")
		return nil
	})

	if err != nil {
		t.Fatal(err.Error())
	}

	payload := make([]byte, 4*1024*1024)
	for i := 0; i < 4*1024*1024; i++ {
		payload[i] = 'x'
	}

	ws.WriteMessage(websocket.TextMessage, payload)

	msgType, msg, err := ws.ReadMessage()
	if err != nil {
		t.Fatal(err.Error())
	}
	if msgType != websocket.TextMessage {
		t.Fatal("invalid msg type", msgType)
	}

	if string(msg) != string(payload) {
		t.Fatal("invalid msg", string(msg))

	}

	time.Sleep(time.Second * 1)
}
