package riak

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"time"
)

// Object is a Riak object
type Object struct {
	Bucket       string            //Object bucket
	Key          string            //Object key
	Ctype        string            //Content-Type
	Vclock       string            //Vclock
	eTag         string            //Etag
	lastModified time.Time         //Last-Modified
	Links        map[string]Link   // Link: <>
	Meta         map[string]string // X-Riak-Meta-*
	Index        map[string]string // X-Riak-Index-*
	Body         *bytes.Buffer     // Body
}

func (o *Object) path() string {
	return "/buckets/" + o.Bucket + "/keys/" + o.Key
}

func (o *Object) hardReset() {
	// clear existing values
	if o.Links != nil {
		for key := range o.Links {
			delete(o.Links, key)
		}
	}
	if o.Meta != nil {
		for key := range o.Meta {
			delete(o.Meta, key)
		}
	}
	if o.Index != nil {
		for key := range o.Index {
			delete(o.Index, key)
		}
	}
	o.Body.Reset()
	o.Bucket, o.Key, o.Ctype, o.Vclock, o.eTag = "", "", "", "", ""
}

// read response headers and body
func (o *Object) fromResponse(res *http.Response) error {
	// clear existing values
	if o.Links != nil {
		for key := range o.Links {
			delete(o.Links, key)
		}
	}
	if o.Meta != nil {
		for key := range o.Meta {
			delete(o.Meta, key)
		}
	}
	if o.Index != nil {
		for key := range o.Index {
			delete(o.Index, key)
		}
	}
	o.Body.Reset()

	// parse header
	for key, vals := range res.Header {
		switch key {
		case "Content-Type":
			o.Ctype = vals[0]
			continue
		case "Last-Modified":
			o.lastModified, _ = time.Parse(time.RFC1123, vals[0])
		case "X-Riak-Vclock":
			o.Vclock = vals[0]
			continue
		case "ETag":
			o.eTag = vals[0]
			continue
		case "Link":
			parseLinks(vals[0], o.Links)
		}
		switch {
		case strings.HasPrefix(key, "X-Riak-Meta-"):
			metakey := strings.SplitAfter(key, "X-Riak-Meta-")[1]
			if o.Meta == nil {
				o.Meta = make(map[string]string)
			}
			o.Meta[metakey] = vals[0]
			continue

		case strings.HasPrefix(key, "X-Riak-Index-"):
			indexkey := strings.SplitAfter(key, "X-Riak-Index-")[1]
			if o.Index == nil {
				o.Index = make(map[string]string)
			}
			o.Index[indexkey] = vals[0]
			continue

		}
	}

	_, err := io.Copy(o.Body, res.Body)
	res.Body.Close()
	return err
}

type Link struct {
	Bucket string
	Key    string
}

func parseLinks(str string, links map[string]Link) {

	return
}

func formatLinks(links map[string]Link) string {

	return ""
}

func (o *Object) writeheader(hd http.Header) {

	if o.Ctype != "" {
		hd.Set("Content-Type", o.Ctype)
	}

	if o.Vclock != "" {
		hd.Set("X-Riak-Vclock", o.Vclock)
	}

	if o.eTag != "" {
		hd.Set("ETag", o.eTag)
	}

	if o.lastModified != 0 {
		hd.Set("Last-Modified", o.lastModified.Format(time.RFC1123))
	}

	if o.Links != nil {
		hd.Set("Link", formatLinks(o.Links))
	}

	if o.Meta != nil {
		for key, val := range o.Meta {
			hd.Set("X-Riak-Meta-"+key, val)
		}
	}

	if o.Index != nil {
		for key, val := range o.Index {
			hd.Set("X-Riak-Index-"+key, val)
		}
	}

	return out
}
