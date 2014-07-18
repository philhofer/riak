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
		Key:    "merge",
		Bucket: "testing",
		Body:   bodyA,
	}

	// write first body
	c := newtestclient("http://localhost:8098")
	err := c.Store(objA, nil)
	if err != nil {
		dump(t, c, err)
	}

	// write second body
	new, err := c.Fetch("testing", "merge", nil)
	if err != nil {
		dump(t, c, err)
	}
	new.Body.WriteString(" more data")
	err = c.Merge(new, nil)
	if err != nil {
		dump(t, c, err)
	}
	Release(new)
	new = nil

	// should fail to merge the first body
	err = c.Merge(objA, nil)
	if err != ErrModified {
		if err == nil {
			t.Fatal("Expected error; got nil.")
		}
		dump(t, c, err)
	}

	up, err := c.GetUpdate(objA, nil)
	if err != nil {
		dump(t, c, err)
	}
	if !up {
		t.Errorf("Expected object to be updated")
	}

	objA.Body.WriteString(" ...yet more.")

	// this should be successful
	err = c.Merge(objA, nil)
	if err != nil {
		dump(t, c, err)
	}
}

func TestUpdate(t *testing.T) {
	/*
		- Write one object
		- Fetch the object, make a change, merge it
		- Call GetUpdate on the first object - it should change
		- Call GetUpdate again; expect no update
	*/

	bodyA := bytes.NewBuffer(nil)
	bodyA.WriteString("Testing, 1, 2, 3")
	objA := &Object{
		Key:    "update",
		Bucket: "testing",
		Body:   bodyA,
	}

	// Create an object
	c := newtestclient("http://localhost:8098")
	err := c.Store(objA, nil)
	if err != nil {
		dump(t, c, err)
	}

	// Fetch the object, make a change, merge it back
	second, err := c.Fetch("testing", "update", nil)
	if err != nil {
		dump(t, c, err)
	}
	second.Body.WriteString(" more data")
	err = c.Merge(second, nil)
	if err != nil {
		dump(t, c, err)
	}

	// Update the first body; we expect to see a change
	up, err := c.GetUpdate(objA, nil)
	if err != nil {
		dump(t, c, err)
	}
	if !up {
		t.Error("Not updated.")
	}

	if objA.Body.String() != "Testing, 1, 2, 3 more data" {
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
