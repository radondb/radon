/*
 * Radon
 *
 * Copyright 2019 The Radon Authors.
 * Code is licensed under the GPLv3.
 *
 */

package xbase

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
)

// makeSimpleRequest used to make a simple http request.
func makeSimpleRequest(method string, url string, payload interface{}) (*http.Request, error) {
	var data string

	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		data = fmt.Sprintf("%s", b)
	}

	r, err := http.NewRequest(method, url, strings.NewReader(data))
	if err != nil {
		return nil, err
	}
	//r.Header.Set("Accept-Encoding", "gzip")
	if payload != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	return r, nil
}

func httpDo(method string, url string, payload interface{}) (*http.Response, func(), error) {
	req, err := makeSimpleRequest(method, url, payload)
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

func HTTPPost(url string, payload interface{}) (*http.Response, func(), error) {
	return httpDo("POST", url, payload)
}

func HTTPPut(url string, payload interface{}) (*http.Response, func(), error) {
	return httpDo("PUT", url, payload)
}

func HTTPReadBody(resp *http.Response) string {
	if resp != nil && resp.Body != nil {
		if bodyBytes, err := ioutil.ReadAll(resp.Body); err != nil {
			return err.Error()
		} else {
			return string(bodyBytes)
		}
	}
	return ""
}
