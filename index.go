package riak

import (
	"bytes"
	"encoding/json"
	"errors"
)

// IndexLookup returns a list of keys in 'bucket' with 'value' for the tag 'index'
func (c *Client) IndexLookup(bucket string, index string, value string) (*Keyres, error) {
	if bucket == "" || index == "" || value == "" {
		return nil, errors.New("Cannot have empty string argument.")
	}
	path := ipath(bucket, index, value)
	res, err := c.do("GET", path, nil)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		res.Body.Close()
		return nil, statusCode(res.StatusCode)
	}
	kr := new(Keyres)
	kr.Keys = make([]string, 0, 1)
	dec := json.NewDecoder(res.Body)
	err = dec.Decode(&kr)
	res.Body.Close()
	return kr, err
}

// /buckets/[bucket]/index/[index]/?...
func ipath(bucket string, index string, value string) string {
	var stack [80]byte
	buf := bytes.NewBuffer(stack[0:0])
	buf.WriteString("/buckets/")
	buf.WriteString(bucket)
	buf.WriteString("/index/")
	buf.WriteString(index)
	buf.WriteByte('/')
	buf.WriteString(value)
	return buf.String()
}

type Keyres struct {
	Keys []string `json:"keys"`
}
