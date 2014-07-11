package riak

import (
	//"bytes"
	//"io/ioutil"
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
	if path != "/buckets/blah/keys/12347" {
		t.Fatalf("Path should be %q; got %q", "/buckets/blah/keys/12347", path)
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
		"ETag":                  []string{obj.eTag},
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

	err := newobj.fromResponse(res)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(obj, newobj) {
		t.Error("The objects are not equal.")
		t.Errorf("Expected: %#v\n", obj)
		t.Errorf("Got: %#v\n", newobj)
	}

}
