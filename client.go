package riak

import (
	"github.com/bitly/go-hostpool"
	"io"
	"net/http"
)

func NewClient(hosts []string) *Client {
	return &Client{
		htc: &http.Client{},
		hp:  hostpool.NewEpsilonGreedy(hosts, 0, &hostpool.LogEpsilonValueCalculator{}),
	}
}

type Client struct {
	htc *http.Client
	hp  hostpool.HostPool
}

func (c *Client) do(method string, path string, body io.Reader) (*http.Response, error) {
	host := c.hp.Get()
	req, err := http.NewRequest(method, host.Host()+path, body)
	if err != nil {
		return nil, err
	}
	res, err := c.htc.Do(req)
	if err != nil {
		host.Mark(err)
	}
	return res, err
}

type req struct {
	htr  *http.Request
	host hostpool.HostPoolResponse
}

func (c *Client) req(method string, path string, body io.Reader) (req, error) {
	host := c.hp.Get()
	htr, err := http.NewRequest(method, host.Host()+path, body)
	if err != nil {
		return req{}, err
	}
	return req{htr, host}, nil
}

func (c *Client) doreq(r req) (*http.Response, error) {
	res, err := c.htc.Do(r.htr)
	if err != nil {
		r.host.Mark(err)
	}
	return res, err
}
