package riak

import (
	"bytes"
	"testing"
	"net/http/httputil"
	"reflect"
)

func TestBasicWriteRead(t *testing.T) {
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

	if !reflect.DeepEqual(obj.Body, newobj.Body) {
		t.Errorf("Object, %#v\n did not match expected %#v\n", newobj, obj)
	}
}

func TestClockConflict(t *testing.T) {
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
	
	_, err = c.Fetch(objB.Bucket, obj.Key, nil)
	if _, ok := err.(*ErrMultipleVclocks); !ok {
		t.Errorf("Expected ErrMultipleVclocks, instead receieved error: %s", err)
	}
}

func TestEmptyBody(t *testing.T) {
	var bodyA bytes.Buffer
	objA := &Object {
		Key: "testKey",
		Bucket: "testing",
		Body: &bodyA,
	}

	bodyB.WriteString("Testing, 1, 2, 3")
	objB := &Object {
		Key: "testKey",
		Bucket: "testing",
		Body: nil,
	}

	c := newtestclient("http://localhost:8098")
	err := c.CreateObject(objA, nil)
	if _, ok := err.(*ErrInvalidBody); !ok {
		t.Errorf("Expected ErrInvalidBody, instead receieved error: %s", err)
	}
	err = c.CreateObject(objB, nil)
	if _, ok := err.(*ErrInvalidBody); !ok {
		t.Errorf("Expected ErrInvalidBody, instead receieved error: %s", err)
}
