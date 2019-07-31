package rpc

import (
	"errors"
	"sync"
	"testing"
)

var testServerOnce sync.Once
var testClientOnce sync.Once

var testServer *Server
var testClient *Client

func newTestServer() *Server {
	f := func() {
		testServer = NewServer("tcp", "127.0.0.1:11182")
		go testServer.Start()
	}

	testServerOnce.Do(f)

	return testServer
}

func newTestClient() *Client {
	f := func() {
		testClient = NewClient("tcp", "127.0.0.1:11182", 10)
	}

	testClientOnce.Do(f)

	return testClient
}

func test_Rpc1(id int) (int, string, error) {
	return id * 10, "abc", nil
}

func TestRpc1(t *testing.T) {
	s := newTestServer()

	s.Register("rpc1", test_Rpc1)

	c := newTestClient()

	var r func(int) (int, string, error)
	if err := c.MakeRpc("rpc1", &r); err != nil {
		t.Fatal(err)
	}

	a, b, e := r(10)
	if e != nil {
		t.Fatal(e)
	}

	if a != 100 || b != "abc" {
		t.Fatal(a, b)
	}
}

func test_Rpc2(ids []int) ([]int, error) {
	if ids == nil || len(ids) == 0 {
		return nil, errors.New("nid ids")
	}

	if len(ids) >= 2 {
		return []int{}, nil
	}

	return []int{ids[0] * 10}, nil
}

func TestRpc2(t *testing.T) {
	s := newTestServer()

	s.Register("rpc2", test_Rpc2)

	c := newTestClient()

	var r func(ids []int) ([]int, error)
	if err := c.MakeRpc("rpc2", &r); err != nil {
		t.Fatal(err)
	}

	a, e := r(nil)
	if e == nil {
		t.Fatal("must error")
	}

	a, e = r([]int{})
	if e == nil {
		t.Fatal("must error")
	}

	a, e = r([]int{1})
	if e != nil {
		t.Fatal(e)
	} else if a[0] != 10 {
		t.Fatal(a[0])
	}

	a, e = r([]int{1, 2, 3})
	if e != nil {
		t.Fatal(e)
	} else if len(a) != 0 {
		t.Fatal("must 0")
	}
}

func test_Rpc3(id int) error {
	return errors.New("hello world")
}

func TestRpc3(t *testing.T) {
	s := newTestServer()

	s.Register("rpc3", test_Rpc3)

	c := newTestClient()

	var r func(int) error
	if err := c.MakeRpc("rpc3", &r); err != nil {
		t.Fatal(err)
	}

	e := r(10)
	if e != nil {
		if e.Error() != "hello world" {
			t.Fatal(e.Error())
		}
	} else {
		t.Fatal("must error")
	}
}
