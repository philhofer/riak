// +build riak

package riak

import (
	"bytes"
	//"net/http/httputil"
	"testing"
)

func TestVclockCollision(t *testing.T) {
	errChan := make(chan error)
	go makeObj("collision", "myKey", "abc", errChan)
	go makeObj("collision", "myKey", "123", errChan)
	errA := <-errChan
	errB := <-errChan

	if (errA == nil) && (errB == nil) {
		c := newtestclient("http://localhost:8098")
		_, err := c.Fetch("collision", "myKey", nil)
		_, ok := err.(*ErrMultipleVclocks)
		if !ok {
			t.Errorf("Expected ErrMultipleVclocks, recieved error: %s\n", err)
		}
	} else {
		_, oka := errA.(*ErrMultipleVclocks)
		_, okb := errB.(*ErrMultipleVclocks)
		if !oka && !okb {
			t.Fatalf("Errors: %s and %s", errA, errB)
		}
	}
}

func makeObj(bucket string, key string, bodyS string, errChan chan error) {
	var body bytes.Buffer
	body.WriteString(bodyS)
	obj := &Object{
		Key:    key,
		Bucket: bucket,
		Body:   &body,
	}
	c := newtestclient("http://localhost:8098")
	c.id = bodyS
	err := c.Store(obj, nil)
	errChan <- err
}
