/*
 * Radon
 *
 * Copyright 2018 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package xbase

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

// makeSimpleRequest used to make a simple http request.
func makeSimpleRequest(ctx context.Context, method string, url string, payload interface{}) (*http.Request, error) {
	var data string

	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		data = fmt.Sprintf("%s", b)
	}

	req, err := http.NewRequest(method, url, strings.NewReader(data))
	if err != nil {
		return nil, err
	}
	//r.Header.Set("Accept-Encoding", "gzip")
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req = req.WithContext(ctx)
	return req, nil
}

func httpDo(method string, url string, payload interface{}) (*http.Response, func(), error) {
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	req, err := makeSimpleRequest(ctx, method, url, payload)
	if err != nil {
		return nil, nil, err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	return resp, func() {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}, err
}

// HTTPPost used to do restful post request.
func HTTPPost(url string, payload interface{}) (*http.Response, func(), error) {
	return httpDo("POST", url, payload)
}

// HTTPPut used to do restful put request.
func HTTPPut(url string, payload interface{}) (*http.Response, func(), error) {
	return httpDo("PUT", url, payload)
}

// HTTPGet used to do restful get request.
func HTTPGet(url string) (string, error) {
	resp, cleanup, err := httpDo("GET", url, nil)
	if err != nil {
		return "", err
	}
	defer cleanup()
	return HTTPReadBody(resp), nil
}

// HTTPReadBody returns the body of the response.
func HTTPReadBody(resp *http.Response) string {
	if resp != nil && resp.Body != nil {
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return err.Error()
		}
		return string(bodyBytes)
	}
	return ""
}
