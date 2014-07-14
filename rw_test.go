// +build riak

package riak

import (
	"bytes"
	"testing"
	"net/http/httputil"
)

func TestUpdate(t *testing.T) {

	// first object
	bodyA := bytes.NewBuffer(nil)
	bodyA.WriteString("Testing, 1, 2, 3")
	objA := &Object {
		Key: "testKey",
		Bucket: "testing",
		Body: bodyA,
	}

	bodyB := bytes.NewBuffer(nil)
	bodyB.WriteString("A second body.")
	objB := &Object {
		Key: "testKey",
		Bucket: "testing",
		Body: bodyB,
	}

	// write first body
	c := newtestclient("http://localhost:8098")
	err := c.Store(objA, nil)
	if err != nil {
		t.Errorf("Recieved error: %s", err)
		body, _ := httputil.DumpRequest(c.lastreq(), false)
		t.Errorf("Last request: %s", body)
		res, _ := httputil.DumpResponse(c.lastres(), false)
		t.Fatalf("Last response: %s", res)
	}

	// write second body
	err = c.Store(objB, nil)
	if err != nil {
		t.Errorf("Recieved error: %s", err)
		body, _ := httputil.DumpRequest(c.lastreq(), false)
		t.Errorf("Last request: %s", body)
		res, _ := httputil.DumpResponse(c.lastres(), false)
		t.Fatalf("Last response: %s", res)
	}

	// update first body
	up, err := c.GetUpdate(objA, nil)
	if err != nil {
		t.Errorf("Recieved error: %s", err)
		body, _ := httputil.DumpRequest(c.lastreq(), false)
		t.Errorf("Last request: %s", body)
		res, _ := httputil.DumpResponse(c.lastres(), false)
		t.Fatalf("Last response: %s", res)
	}

	if !up {
		t.Error("Not updated.")
	}

	if objA.Body.String() != "A second body." {
		t.Fatalf("Expected body %q, got %q", "A second body.", objA.Body.String())
	}

}

func TestCreateFetch(t *testing.T) {
	var body bytes.Buffer
	body.WriteString("Testing, 1, 2, 3")
	obj := &Object {
		Bucket: "testing",
		Body: &body,
	}
	c := newtestclient("http://localhost:8098")
	err := c.CreateObject(obj, nil)
	if err != nil {
		t.Errorf("Recieved error: %s", err)
		body, _ := httputil.DumpRequest(c.lastreq(), false)
		t.Errorf("Last request: %s", body)
		res, _ := httputil.DumpResponse(c.lastres(), false)
		t.Fatalf("Last response: %s", res)
	}

	newobj, err := c.Fetch(obj.Bucket, obj.Key, nil)
	if err != nil {
		t.Errorf("Recieved error: %s", err)
		body, _ := httputil.DumpRequest(c.lastreq(), false)
		t.Errorf("Last request: %s", body)
		res, _ := httputil.DumpResponse(c.lastres(), false)
		t.Fatalf("Last response: %s", res)
	}

	if newobj.Body.String() != "Testing, 1, 2, 3" {
		t.Errorf("Object body \n%s\n did not match expected \n%s\n", newobj.Body.String(), "Testing, 1, 2, 3")
	}
	Release(obj)
	Release(newobj)
}

func TestDoubleStore(t *testing.T) {
	var bodyA bytes.Buffer
	bodyA.WriteString("Testing, 1, 2, 3")
	objA := &Object {
		Key: "testKey",
		Bucket: "testing",
		Body: &bodyA,
	}

	var bodyB bytes.Buffer
	bodyB.WriteString("Testing, 1, 2, 3")
	objB := &Object {
		Key: "testKey",
		Bucket: "testing",
		Body: &bodyB,
	}

	c := newtestclient("http://localhost:8098")
	err := c.Store(objA, nil)
	if err != nil {
		t.Errorf("Recieved error: %s", err)
		body, _ := httputil.DumpRequest(c.lastreq(), false)
		t.Errorf("Last request: %s", body)
		res, _ := httputil.DumpResponse(c.lastres(), false)
		t.Fatalf("Last response: %s", res)
	}
	err = c.Store(objB, nil)
	if err != nil {
		t.Errorf("Recieved error: %s", err)
		body, _ := httputil.DumpRequest(c.lastreq(), false)
		t.Errorf("Last request: %s", body)
		res, _ := httputil.DumpResponse(c.lastres(), false)
		t.Fatalf("Last response: %s", res)
	}

	_, err = c.Fetch(objB.Bucket, objB.Key, nil)
	if err != nil {
		t.Fatalf("Error on fetch: %s", err)
		body, _ := httputil.DumpRequest(c.lastreq(), false)
		t.Errorf("Last request: %s", body)
		res, _ := httputil.DumpResponse(c.lastres(), false)
		t.Fatalf("Last response: %s", res)
	}
}

func TestEmptyBody(t *testing.T) {
	var bodyA bytes.Buffer
	objA := &Object {
		Key: "testKey",
		Bucket: "testing",
		Body: &bodyA,
	}

	objB := &Object {
		Key: "testKey",
		Bucket: "testing",
		Body: nil,
	}

	c := newtestclient("http://localhost:8098")
	err := c.CreateObject(objA, nil)
	if _, ok := err.(ErrInvalidBody); !ok {
		t.Errorf("Expected ErrInvalidBody, instead receieved error: %s", err)
	}
	err = c.CreateObject(objB, nil)
	if _, ok := err.(ErrInvalidBody); !ok {
		t.Errorf("Expected ErrInvalidBody, instead receieved error: %s", err)
	}
}
