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

// FollowLink follows one of the object's named links
func (c *Client) FollowLink(o *Object, name string) ([]*Object, error) {
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
		default:
			return nil, statusCode(res.StatusCode)
		}
	}
	var objs []*Object
	mtype, params, err := mime.ParseMediaType(res.Header.Get("Content-Type"))
	if err != nil {
		return nil, err
	}

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
			// TODO
			// TODO
			// TODO
			// TODO
			//
		}
	}
	return nil, nil
}

func linkpath(o *Object, name string, link Link) string {
	buf := bytes.NewBuffer(nil)
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
