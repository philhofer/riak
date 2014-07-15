// +build riak

package riak

import (
	"bytes"
	//"net/http/httputil"
	"testing"
)

func TestVclockCollision(t *testing.T) {
	errChan := make(chan error)
	go makeObj("bucket", "key", "abc", errChan)
	go makeObj("bucket", "key", "123", errChan)
	errA := <- errChan
	errB := <- errChan

	if (errA == nil) && (errB == nil) {
		c := newtestclient("http://localhost:8098")
		_, err := c.Fetch("bucket", "key", nil)
		_, ok := err.(*ErrMultipleVclocks)
		if !ok {
			t.Errorf("Expected ErrMultipleVclocks, recieved error: %s\n", err) 
		}
	}
}

func makeObj(bucket string, key string, bodyS string, errChan chan error) {
	var body bytes.Buffer
	body.WriteString(bodyS)
	obj := &Object{
		Key: key,
		Bucket: bucket,
		Body:   &body,
	}
	c := newtestclient("http://localhost:8098")
	c.id = bodyS
	err := c.Store(obj, nil)
	errChan <- err
}

