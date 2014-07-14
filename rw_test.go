// +build riak

package riak

import (
	"bytes"
	"net/http/httputil"
	"testing"
)

func dump(t *testing.T, c *Client, err error) {
	t.Errorf("Error: %s", err)
	body, _ := httputil.DumpRequest(c.lastreq(), false)
	t.Errorf("Last request: %s", body)
	res, _ := httputil.DumpResponse(c.lastres(), false)
	t.Fatalf("Last response: %s", res)
}

func TestMerge(t *testing.T) {
	/*
		- Write one object
		- Write a second object at the same key
		- Try to merge changes from the first object (should fail ErrModified)
		- Try to merge changes from the second object (should succeed.)
	*/

	bodyA := bytes.NewBuffer(nil)
	bodyA.WriteString("Testing, 1, 2, 3")
	objA := &Object{
		Key:    "testKey",
		Bucket: "testing",
		Body:   bodyA,
	}

	bodyB := bytes.NewBuffer(nil)
	bodyB.WriteString("A second body.")
	objB := &Object{
		Key:    "testKey",
		Bucket: "testing",
		Body:   bodyB,
	}

	// write first body
	c := newtestclient("http://localhost:8098")
	err := c.Store(objA, nil)
	if err != nil {
		dump(t, c, err)
	}

	// write second body
	err = c.Store(objB, nil)
	if err != nil {
		dump(t, c, err)
	}

	// should fail to merge the first body
	err = c.Merge(objA, nil)
	if err != ErrModified {
		if err == nil {
			t.Fatal("Expected error; got nil.")
		}
		dump(t, c, err)
	}

	objB.Body.Reset()
	objB.Body.WriteString("A second body.")

	// this should be successful
	err = c.Merge(objB, nil)
	if err != nil {
		dump(t, c, err)
	}
}

func TestUpdate(t *testing.T) {
	/*
		- Write one object
		- Write a second object at the same key
		- Call GetUpdate on the first object, verify equality with second object
		- Call GetUpdate again; expect no update
	*/

	bodyA := bytes.NewBuffer(nil)
	bodyA.WriteString("Testing, 1, 2, 3")
	objA := &Object{
		Key:    "testKey",
		Bucket: "testing",
		Body:   bodyA,
	}

	bodyB := bytes.NewBuffer(nil)
	bodyB.WriteString("A second body.")
	objB := &Object{
		Key:    "testKey",
		Bucket: "testing",
		Body:   bodyB,
	}

	// write first body
	c := newtestclient("http://localhost:8098")
	err := c.Store(objA, nil)
	if err != nil {
		dump(t, c, err)
	}

	// write second body
	err = c.Store(objB, nil)
	if err != nil {
		dump(t, c, err)
	}

	// update first body
	up, err := c.GetUpdate(objA, nil)
	if err != nil {
		dump(t, c, err)
	}

	if !up {
		t.Error("Not updated.")
	}

	if objA.Body.String() != "A second body." {
		t.Fatalf("Expected body %q, got %q", "A second body.", objA.Body.String())
	}

	// now test for not updated
	up, err = c.GetUpdate(objA, nil)
	if err != nil {
		dump(t, c, err)
	}
	if up {
		t.Errorf("Object was updated unexpectedly.")
	}

}

func TestCreateFetch(t *testing.T) {
	/*
		- Create an object
		- Fetch with returned bucket/key
	*/
	var body bytes.Buffer
	body.WriteString("Testing, 1, 2, 3")
	obj := &Object{
		Bucket: "testing",
		Body:   &body,
	}
	c := newtestclient("http://localhost:8098")
	err := c.CreateObject(obj, nil)
	if err != nil {
		dump(t, c, err)
	}

	newobj, err := c.Fetch(obj.Bucket, obj.Key, nil)
	if err != nil {
		dump(t, c, err)
	}

	if newobj.Body.String() != "Testing, 1, 2, 3" {
		t.Errorf("Object body \n%s\n did not match expected \n%s\n", newobj.Body.String(), "Testing, 1, 2, 3")
	}
	Release(obj)
	Release(newobj)
}

func TestDoubleStore(t *testing.T) {
	/* Test sequential writes w/o vclocks
	to determine that Riak does actually
	take care of the conflict. */
	var bodyA bytes.Buffer
	bodyA.WriteString("Testing, 1, 2, 3")
	objA := &Object{
		Key:    "testKey",
		Bucket: "testing",
		Body:   &bodyA,
	}

	var bodyB bytes.Buffer
	bodyB.WriteString("Testing, 1, 2, 3")
	objB := &Object{
		Key:    "testKey",
		Bucket: "testing",
		Body:   &bodyB,
	}

	c := newtestclient("http://localhost:8098")
	err := c.Store(objA, nil)
	if err != nil {
		dump(t, c, err)
	}
	err = c.Store(objB, nil)
	if err != nil {
		dump(t, c, err)
	}

	_, err = c.Fetch(objB.Bucket, objB.Key, nil)
	if err != nil {
		dump(t, c, err)
	}
}

func TestEmptyBody(t *testing.T) {
	// Sending an empty body is an error

	var bodyA bytes.Buffer
	objA := &Object{
		Key:    "testKey",
		Bucket: "testing",
		Body:   &bodyA,
	}

	objB := &Object{
		Key:    "testKey",
		Bucket: "testing",
		Body:   nil,
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
