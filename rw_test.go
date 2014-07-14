package riak

import (
	"bytes"
	"testing"
)

func TestBasicWrite(t *testing.T) {
	var body bytes.Buffer
	body.WriteString("Testing, 1, 2, 3")
	obj := &Object { 
		Bucket: "testing",
		Body: &body,
	}
	c := NewClient("http://localhost:8098", "tester") 
	err := c.Store(obj, nil)
	if err != nil {
		t.Fatalf("Recieved error: %s", err)
	}
}

