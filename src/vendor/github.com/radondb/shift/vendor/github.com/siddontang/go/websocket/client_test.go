package websocket

import (
	"net"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestWSClient(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	http.HandleFunc("/test/client", func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			t.Log("server: wg.Done")
			wg.Done()
		}()

		conn, err := websocket.Upgrade(w, r, nil, 1024, 1024)
		if err != nil {
			t.Fatal(err.Error())
		}
		t.Log("websocket.Upgrade")

		conn.SetPingHandler(func(d string) error {
			t.Log("receive from client: ", d)
			conn.WriteMessage(websocket.PongMessage, []byte("server.Pong"))
			return nil
		})

		msgType, msg, err := conn.ReadMessage()
		if err != nil {
			t.Fatal(err.Error())
		}
		if msgType != websocket.TextMessage {
			t.Fatal("invalid msg type", msgType)
		}

		err = conn.WriteMessage(websocket.TextMessage, msg)
		if err != nil {
			t.Fatal(err.Error())
		}

		msgType, msg, err = conn.ReadMessage()
		if err != nil {
			t.Fatal(err.Error())
		}
		if msgType != websocket.TextMessage {
			t.Fatal("invalid msg type", msgType)
		}
		conn.WriteMessage(websocket.PongMessage, []byte("server.Pong"))
	})

	go http.ListenAndServe(":65500", nil)

	time.Sleep(time.Second * 1)

	conn, err := net.Dial("tcp", "127.0.0.1:65500")

	if err != nil {
		t.Fatal(err.Error())
	}
	ws, _, err := NewClient(conn, &url.URL{Scheme: "ws", Host: "127.0.0.1:65501", Path: "/test/client"}, nil)

	if err != nil {
		t.Fatal(err.Error())
	}

	payload := make([]byte, 4*1024)
	for i := 0; i < 4*1024; i++ {
		payload[i] = 'x'
	}

	err = ws.WriteString(payload)
	if err != nil {
		t.Fatal(err.Error())
	}

	msgType, msg, err := ws.ReadMessage()
	if err != nil {
		t.Fatal(err.Error())
	}
	if msgType != TextMessage {
		t.Fatal("invalid msg type", msgType)
	}

	if string(msg) != string(payload) {
		t.Fatal("invalid msg", string(msg))
	}

	//test ping
	err = ws.Ping([]byte("client.Ping"))
	if err != nil {
		t.Fatal(err.Error())
	}

	msgType, msg, err = ws.ReadMessage()
	if err != nil {
		t.Fatal(err.Error())
	}
	if msgType != PongMessage {
		t.Fatal("invalid msg type", msgType)
	}

	ws.WriteMessage(websocket.TextMessage, []byte("done"))

	// ws.Close()
	wg.Wait()
}
