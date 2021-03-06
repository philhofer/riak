package riak

import (
	"bytes"
	"net/http"
	"reflect"
	"testing"
	"time"
)

func TestObjectPath(t *testing.T) {
	obj := &Object{
		Bucket: "blah",
		Key:    "12347",
	}

	path := obj.path()
	if path != "/riak/blah/12347" {
		t.Fatalf("Path should be %q; got %q", "/riak/blah/12347", path)
	}
}

func TestObjectIndexAccessors(t *testing.T) {
	obj := &Object{}
	pairs := map[string]string{
		"indexKey":    "value",
		"CAPSKEY":     "otherval",
		"lower_snake": "whaarrrgarbbblll",
	}
	for key, val := range pairs {
		obj.AddIndex(key, val)
	}
	hdr := make(http.Header)
	obj.writeheader(hdr)
	newob := new(Object)
	newob.fromResponse(hdr, nil)
	for key, val := range pairs {
		out := newob.GetIndex(key)
		if out != val {
			t.Errorf("Key-pair %q:%q retreived as %q:%q", key, val, key, out)
		}
	}
}

func TestObjectWriteHeader(t *testing.T) {
	tm := time.Now()
	obj := &Object{
		Ctype:        "text/plain",
		Vclock:       "125g85gu90[g89-]",
		eTag:         "h801235hi0ggasty890",
		lastModified: tm,
		Links:        map[string]Link{"result": {Bucket: "blah", Key: "rs1"}},
		Meta:         map[string]string{"Agent": "testing"},
		Index:        map[string]string{"Username": "Bob"},
	}

	header := make(http.Header)

	obj.writeheader(header)

	wanted := http.Header{
		"Content-Type":          []string{obj.Ctype},
		"X-Riak-Vclock":         []string{obj.Vclock},
		"Etag":                  []string{obj.eTag},
		"Last-Modified":         []string{tm.Format(time.RFC1123)},
		"Link":                  []string{"</riak/blah/rs1>; riaktag=\"result\""},
		"X-Riak-Meta-Agent":     []string{"testing"},
		"X-Riak-Index-Username": []string{"Bob"},
	}

	if !reflect.DeepEqual(header, wanted) {
		t.Error("Headers not equal.")
		for key := range wanted {
			if !reflect.DeepEqual(header[key], wanted[key]) {
				t.Errorf("Expected key %q to produce %#v; got %#v", key, wanted[key], header[key])
			}
		}
	}
}

func TestObjectReadHeader(t *testing.T) {
	tm, _ := time.Parse(time.RFC1123, time.Now().Format(time.RFC1123))
	obj := &Object{
		Ctype:        "text/plain",
		Vclock:       "125g85gu90[g89-]",
		eTag:         "h801235hi0ggasty890",
		lastModified: tm,
		Links:        map[string]Link{"result": {Bucket: "blah", Key: "rs1"}, "other": {Bucket: "things", Key: "j90"}},
		Meta:         map[string]string{"Agent": "testing"},
		Index:        map[string]string{"Username": "Bob"},
		Body:         nil,
	}

	header := make(http.Header)
	obj.writeheader(header)

	res := &http.Response{
		Header: header,
		Body:   nil,
	}

	newobj := new(Object)

	err := newobj.fromResponse(res.Header, res.Body)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(obj, newobj) {
		t.Error("The objects are not equal.")
		t.Errorf("Expected: %#v\n", obj)
		t.Errorf("Got: %#v\n", newobj)
	}
	Release(newobj)
}

func TestObjectHardReset(t *testing.T) {
	tm := time.Now()
	obj := &Object{
		Ctype:        "text/plain",
		Vclock:       "125g85gu90[g89-]",
		eTag:         "h801235hi0ggasty890",
		lastModified: tm,
		Links:        map[string]Link{"result": {Bucket: "blah", Key: "rs1"}, "other": {Bucket: "things", Key: "j90"}},
		Meta:         map[string]string{"Agent": "testing"},
		Index:        map[string]string{"Username": "Bob"},
		Body:         bytes.NewBuffer(nil),
	}

	resetobj := &Object{
		Body: bytes.NewBuffer(nil),
	}

	obj.hardReset()

	if !objectEqual(obj, resetobj) {
		t.Error("Objects are not equivalent.")
		t.Errorf("obj: %#v", obj)
		t.Errorf("resetobj: %#v", resetobj)
	}

	Release(obj)
	Release(resetobj)
}
