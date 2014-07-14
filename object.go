package riak

import (
	"bytes"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
)

var (
	linkrgx *regexp.Regexp
)

func init() {
	rgx, err := regexp.Compile("(</riak/(.*?)/(.*?)>;\\sriaktag=\"(.*?)\")")
	if err != nil {
		panic(err)
	}
	linkrgx = rgx
}

// Object is a Riak object. Its fields
// represent the data associated with a riak object, as well
// as the acutal body of the object. Please note that this
// package does not escape strings before forming url paths,
// so you must properly url-escape your strings if you are
// using unsupported characters.
type Object struct {
	Bucket       string            // Object bucket
	Key          string            // Object key
	Ctype        string            // Content-Type
	Vclock       string            // Last seen vector clock
	eTag         string            // Etag
	lastModified time.Time         // Last-Modified
	Links        map[string]Link   // Link: </riak/bucket/key>
	Meta         map[string]string // X-Riak-Meta-*
	Index        map[string]string // X-Riak-Index-*
	Body         *bytes.Buffer     // Body
}

func objectEqual(on *Object, of *Object) bool {
	if on == of {
		return true
	}

	if on.Bucket != of.Bucket || on.Key != of.Key || on.Ctype != of.Ctype || on.Vclock != of.Vclock || on.eTag != of.eTag {
		return false
	}

	if !on.lastModified.Equal(of.lastModified) {
		return false
	}

	// we're treating nil maps and empty maps as the same
	if on.Links == nil {
		if of.Links != nil {
			if len(of.Links) == 0 {
				goto meta
			}
			return false
		} else {
			goto meta
		}
	} else if of.Links == nil {
		if len(on.Links) == 0 {
			goto meta
		}
		return false
	}

	if len(on.Links) != len(of.Links) {
		return false
	}

	for key, val := range on.Links {
		tval, ok := of.Links[key]
		if !ok {
			return false
		}
		if tval != val {
			return false
		}
	}

	for key, val := range of.Links {
		tval, ok := on.Links[key]
		if !ok {
			return false
		}
		if tval != val {
			return false
		}
	}

	// META
meta:
	if on.Meta == nil {
		if of.Meta != nil {
			if len(of.Meta) == 0 {
				goto index
			}
			return false
		} else {
			goto index
		}
	} else if of.Meta == nil {
		if len(on.Meta) == 0 {
			goto index
		}
		return false
	}

	if len(on.Meta) != len(of.Meta) {
		return false
	}

	for key, val := range on.Meta {
		tval, ok := of.Meta[key]
		if !ok {
			return false
		}
		if tval != val {
			return false
		}
	}

	for key, val := range of.Meta {
		tval, ok := on.Meta[key]
		if !ok {
			return false
		}
		if tval != val {
			return false
		}
	}

index:
	if on.Index == nil {
		if of.Index == nil {
			goto body
		} else if len(of.Index) == 0 {
			goto body
		}
		return false
	} else if of.Index == nil {
		if len(on.Index) == 0 {
			goto body
		}
		return false
	}

	for key, val := range on.Index {
		tval, ok := of.Index[key]
		if !ok {
			return false
		}
		if tval != val {
			return false
		}
	}

	for key, val := range of.Index {
		tval, ok := on.Index[key]
		if !ok {
			return false
		}
		if tval != val {
			return false
		}
	}

body:
	if on.Body == nil {
		if of.Body == nil {
			return true
		} else if len(of.Body.Bytes()) == 0 {
			return true
		}
		return false
	} else if of.Body == nil {
		if len(on.Body.Bytes()) == 0 {
			return true
		}
		return false
	}
	if len(on.Body.Bytes()) != len(of.Body.Bytes()) {
		return false
	}
	abts, bbts := on.Body.Bytes(), of.Body.Bytes()
	for i, v := range abts {
		if bbts[i] != v {
			return false
		}
	}

	return true
}

func (o *Object) path() string {
	var stack [64]byte
	buf := bytes.NewBuffer(stack[0:0])
	buf.WriteString("/riak/")
	buf.WriteString(o.Bucket)
	buf.WriteByte('/')
	buf.WriteString(o.Key)
	return buf.String()
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
	if o.Body != nil {
		o.Body.Reset()
	}
	o.lastModified = time.Time{}
	o.Bucket, o.Key, o.Ctype, o.Vclock, o.eTag = "", "", "", "", ""
}

// read response headers and body
// only deletes old values if it finds new ones
func (o *Object) fromResponse(hdr map[string][]string, body io.ReadCloser) error {
	// parse header
	for key, vals := range hdr {
		// handle empty header field...
		if len(vals) < 1 {
			continue
		}

		// regular headers
		switch key {
		case "Content-Type":
			o.Ctype = vals[0]
			continue
		case "Last-Modified":
			o.lastModified, _ = time.Parse(time.RFC1123, vals[0])
		case "X-Riak-Vclock":
			o.Vclock = vals[0]
			continue
		case "Etag":
			o.eTag = vals[0]
			continue
		case "Link":
			if o.Links != nil {
				for key := range o.Links {
					delete(o.Links, key)
				}
			}
			for _, val := range vals {
				parseLinks(val, &o.Links)
			}
		}

		// meta and index maps
		// remove old map if we find the header
		switch {
		case strings.HasPrefix(key, "X-Riak-Meta-"):
			if o.Meta != nil {
				for key := range o.Meta {
					delete(o.Meta, key)
				}
			} else {
				o.Meta = make(map[string]string)
			}
			metakey := strings.SplitAfter(key, "X-Riak-Meta-")[1]
			o.Meta[metakey] = vals[0]
			continue

		case strings.HasPrefix(key, "X-Riak-Index-"):
			if o.Index != nil {
				for key := range o.Index {
					delete(o.Index, key)
				}
			} else {
				o.Index = make(map[string]string)
			}
			indexkey := strings.SplitAfter(key, "X-Riak-Index-")[1]
			o.Index[indexkey] = vals[0]
			continue

		}
	}
	if body == nil {
		return nil
	}
	if o.Body != nil {
		o.Body.Reset()
	} else {
		o.Body = bytes.NewBuffer(nil)
	}
	_, err := io.Copy(o.Body, body)
	body.Close()
	return err
}

type Link struct {
	Bucket string
	Key    string
}

func parseLinks(str string, links *map[string]Link) {
	matches := linkrgx.FindAllStringSubmatch(str, -1)
	if len(matches) == 0 {
		return
	}
	if links == nil || *links == nil {
		*links = make(map[string]Link)
	}
	for _, match := range matches {
		if len(match) < 5 {
			panic("match length < 5")
		}
		(*links)[match[4]] = Link{Bucket: match[2], Key: match[3]}
	}
	return
}

func formatLinks(links map[string]Link) string {
	i := 0
	buf := bytes.NewBuffer(make([]byte, 64)[0:0])
	for key, link := range links {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("</riak/")
		buf.WriteString(link.Bucket)
		buf.WriteByte('/')
		buf.WriteString(link.Key)
		buf.WriteString(">; riaktag=\"")
		buf.WriteString(key)
		buf.WriteString("\"")
		i++
	}
	return buf.String()
}

func (o *Object) writeheader(hd http.Header) {

	if o.Ctype != "" {
		hd.Set("Content-Type", o.Ctype)
	} else {
		hd.Set("Content-Type", "text/plain")
	}

	if o.Vclock != "" {
		hd.Set("X-Riak-Vclock", o.Vclock)
	}

	if o.eTag != "" {
		hd.Set("Etag", o.eTag)
	}

	if o.lastModified.Unix() != 0 {
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

}
