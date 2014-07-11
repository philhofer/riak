package riak

import (
	"io"
	"net/http"
)

func NewClient(host string) *Client {
	return &Client{
		cl:   &http.Client{},
		host: host,
	}
}

type Client struct {
	cl   *http.Client
	host string
}

func (c *Client) do(method string, path string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, c.host+path, body)
	if err != nil {
		return nil, err
	}
	res, err := c.cl.Do(req)
	return res, err
}
