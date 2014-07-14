package riak

import (
	"bytes"
	"testing"
	"net/http/httputil"
)

func TestBasicWriteRead(t *testing.T) {
	var body bytes.Buffer
	body.WriteString("Testing, 1, 2, 3")
	obj := &Object {
		Bucket: "testing",
		Body: &body,
	}
	c := newtestclient("http://localhost:8098")
	err := c.CreateObject(obj)
	if err != nil {
		t.Errorf("Recieved error: %s", err)
		body, _ := httputil.DumpRequest(c.lastreq(), false)
		t.Errorf("Last request: %s", body)
		res, _ := httputil.DumpResponse(c.lastres(), false)
		t.Fatalf("Last response: %s", res)
	}

	newobj, err := c.Fetch(obj.path(), nil)
	if err != nil {
		t.Errorf("Recieved error: %s", err)
		body, _ := httputil.DumpRequest(c.lastreq(), false)
		t.Errorf("Last request: %s", body)
		res, _ := httputil.DumpResponse(c.lastres(), false)
		t.Fatalf("Last response: %s", res)
	}

	if !objectEqual(obj, newobj) {
		t.Errorf("Object, %#v\n did not match expected %#v\n", newobj, obj)
	}
}

