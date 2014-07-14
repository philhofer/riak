package riak

import (
	"bytes"
	"testing"
	"net/http/httputil"
)

func TestBasicWrite(t *testing.T) {
	var body bytes.Buffer
	body.WriteString("Testing, 1, 2, 3")
	obj := &Object {
		Bucket: "testing",
		Body: &body,
	}
	c := newtestclient("http://localhost:8098")
	err := c.Store(obj, nil)
	if err != nil {
		t.Errorf("Recieved error: %s", err)
		body, _ := httputil.DumpRequest(c.lastreq(), false)
		t.Errorf("Last request: %s", body)
		res, _ := httputil.DumpResponse(c.lastres(), false)
		t.Errorf("Last response: %s", res)
	}
}
