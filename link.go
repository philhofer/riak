package riak

import (
	"bytes"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"
)

// FollowMultiLink follows one of the object's named links, returning
// one or many Objects.
func (c *Client) FollowMultiLink(o *Object, name string) ([]*Object, error) {
	link, ok := o.Links[name]
	if !ok {
		return nil, errors.New("Link name doesn't exist for this object.")
	}
	path := linkpath(o, name, link)

	req, err := http.NewRequest("GET", c.host+path, nil)
	req.Header.Set("X-Riak-ClientId", c.id)

	res, err := c.cl.Do(req)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != 200 {
		res.Body.Close()
		switch res.StatusCode {
		case 400:
			return nil, ErrBadRequest
		case 404:
			return nil, ErrNotFound
		default:
			return nil, statusCode(res.StatusCode)
		}
	}
	var objs []*Object
	mtype, params, err := mime.ParseMediaType(res.Header.Get("Content-Type"))
	if err != nil {
		return nil, err
	}

	var deferError error
	if strings.HasPrefix(mtype, "multipart/") {
		mpr := multipart.NewReader(res.Body, params["boundary"])
		for {
			part, err := mpr.NextPart()
			if err != nil {
				if err == io.EOF {
					break
				}
				res.Body.Close()
				return objs, err
			}

			o := newObj()
			err = o.fromResponse(part.Header, part)
			if err != nil {
				deferError = err
				continue
			}
		}
	} else {
		o := newObj()
		err = o.fromResponse(res.Header, res.Body)
		objs = append(objs, o)
		return objs, err
	}
	return objs, deferError
}

func linkpath(o *Object, name string, link Link) string {
	var stack [64]byte
	buf := bytes.NewBuffer(stack[0:0])
	buf.WriteString("/riak/")
	buf.WriteString(o.Bucket)
	buf.WriteByte('/')
	buf.WriteString(o.Key)
	buf.WriteByte('/')

	if link.Bucket != "" {
		buf.WriteString(link.Bucket)
	} else {
		buf.WriteByte('_')
	}
	buf.WriteByte(',')

	buf.WriteString(name)
	buf.WriteByte(',')

	if link.Key != "" {
		buf.WriteString(link.Key)
	} else {
		buf.WriteByte('_')
	}
	return buf.String()
}

// FetchLink follows an object link that links to one object.
// This works analagously to Fetch()ing the object at the named link. 'opts'
// are passed directly to Fetch. The link must have both the bucket and key fields defined.
func (c *Client) FetchLink(o *Object, name string, opts map[string]string) (*Object, error) {
	link, ok := o.Links[name]
	if !ok {
		return nil, errors.New("Link name doesn't exist for this object.")
	}

	if link.Bucket == "" || link.Key == "" {
		return nil, errors.New("Link doesn't link to one object.")
	}

	return c.Fetch(link.Bucket, link.Key, opts)
}
