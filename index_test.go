// +build riak

package riak

import (
	"bytes"
	"testing"
)

func TestIndexLookup(t *testing.T) {
	bodyA := bytes.NewBuffer(nil)
	bodyA.WriteString("Testing, 1, 2, 3")
	objA := &Object{
		Key:    "indexer",
		Bucket: "testing",
		Body:   bodyA,
	}

	objA.AddIndex("USERNAME_bin", "bob123")

	c := newtestclient("http://localhost:8098")
	err := c.Store(objA, nil)
	if err != nil {
		dump(t, c, err)
	}
	Release(objA)
	objA = nil

	new, err := c.Fetch("testing", "indexer", nil)
	if err != nil {
		dump(t, c, err)
	}

	idx := new.GetIndex("username_bin")
	if idx == "" {
		t.Errorf("Fetched object has indexes %v", new.Index)
		t.Fatalf("Fetched object does not have proper 2i.")
	}
	if idx != "bob123" {
		t.Fatalf("Expected %q under tag %q; found %q", "bob123", "username_bin", idx)
	}

	// now lets look up username = bob
	keys, err := c.IndexLookup("testing", "USERNAME_bin", "bob123")
	if err != nil {
		dump(t, c, err)
	}

	if len(keys.Keys) != 1 {
		t.Errorf("We expected 1 key returned; got %d", len(keys.Keys))
	}
	if len(keys.Keys) == 0 {
		dump(t, c, err)
	}

	if keys.Keys[0] != "indexer" {
		t.Errorf("Got key %q; expected %q", keys.Keys[0], "bob123")
	}
}
