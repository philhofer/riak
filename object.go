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

// Object is a Riak object
type Object struct {
	Bucket       string            // Object bucket
	Key          string            // Object key
	Ctype        string            // Content-Type
	Vclock       string            // Vclock
	eTag         string            // Etag
	lastModified time.Time         // Last-Modified
	Links        map[string]Link   // Link: <>
	Meta         map[string]string // X-Riak-Meta-*
	Index        map[string]string // X-Riak-Index-*
	Body         *bytes.Buffer     // Body
}

func (o *Object) path() string {
	return "/riak/" + o.Bucket + "/" + o.Key
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
func (o *Object) fromResponse(hdr map[string][]string, body io.ReadCloser) error {
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

	// parse header
	for key, vals := range hdr {
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
			for _, val := range vals {
				parseLinks(val, &o.Links)
			}
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
	if body == nil {
		return nil
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
