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
	
	_, okA := errA.(*ErrMultipleVclocks)
	_, okB := errB.(*ErrMultipleVclocks)
	
	if (!okA && !okB) { 
		t.Errorf("Expected ErrInvalidBody, instead receieved errors:\n%s\n%s\n", errA, errB)
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

