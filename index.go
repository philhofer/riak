package riak

import (
	"bytes"
	"encoding/json"
	"errors"
)

// SearchIndex returns a list of keys in the bucket 'bucket' with the 'index'
// secondary index field.
func (c *Client) IndexKeys(bucket string, index string, value string) ([]string, error) {
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
	var kr keyres
	dec := json.NewDecoder(res.Body)
	err = dec.Decode(&kr)
	res.Body.Close()
	return kr.keys, err
}

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

type keyres struct {
	keys []string `json:"keys"`
}
