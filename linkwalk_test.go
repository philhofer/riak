// +build riak

package riak

import (
	"bytes"
	"testing"
)

func TestLinkWalk(t *testing.T) {
	/*
	   - Create an object
	   - Create a second object
	   - Link first object to second object and merge
	   - Walk link directly to second object through first object and compare bodies
	*/
	bodyA := bytes.NewBuffer(nil)
	bodyA.WriteString("Testing, 1, 2, 3")
	objA := &Object{
		Key:    "testKey",
		Bucket: "testing",
		Body:   bodyA,
	}

	c := newtestclient("http://localhost:8098")
	err := c.Store(objA, nil)
	if err != nil {
		dump(t, c, err)
	}

	bodyB := bytes.NewBuffer(nil)
	bodyB.WriteString("A second body.")
	objB := &Object{
		Key:    "anotherKey",
		Bucket: "testing",
		Body:   bodyB,
	}

	err = c.Store(objB, nil)
	if err != nil {
		dump(t, c, err)
	}

	objA.AddLink("child", objB)

	err = c.Merge(objA, nil)
	if err != nil {
		dump(t, c, err)
	}

	child, err := c.FetchLink(objA, "child", nil)
	if err != nil {
		dump(t, c, err)
	}

	if child.Body.String() != bodyB.String() {
		t.Errorf("Child does not equal bodyB.")
		t.Errorf("Got %q; expected %q.", child.Body.String(), bodyB.String())
	}

	Release(objA)
	Release(objB)
	Release(child)
}
