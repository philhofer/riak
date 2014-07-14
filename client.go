package riak

import (
	"io"
	"net/http"
)

func NewClient(host string, clientID string) *Client {
	return &Client{
		cl:   &http.Client{},
		host: host,
		id:   clientID,
	}
}

// FOR TESTING
type doer interface {
	Do(*http.Request) (*http.Response, error)
}

// FOR TESTING
type testDo struct {
	client  *http.Client
	lastReq *http.Request
}

// FOR TESTING
func (t *testDo) Do(req *http.Request) (*http.Response, error) {
	t.lastReq = req
	return t.client.Do(req)
}

// FOR TESTING
func newtestclient(host string) *Client {
	return &Client{
		cl: &testDo{
			client:  &http.Client{},
			lastReq: nil,
		},
		host: host,
		id:   "testClient",
	}
}

// FOR TESTING
func (c *Client) lastreq() *http.Request {
	return c.cl.(*testDo).lastReq
}

type Client struct {
	cl   doer
	host string
	id   string
}

// only for bucket props, etc.
func (c *Client) do(method string, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, c.host+path, body)
	if err != nil {
		return nil, err
	}
	res, err := c.cl.Do(req)
	return res, err
}
