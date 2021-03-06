package riak

import (
	"net/http"
	"net/url"
	"strings"
)

// CreateObject creates a new object in 'bucket' and modifies the object
// key to be the key that riak assigned it. Only the 'body' and 'bucket'
// fields of the object need to be defined. Valid options are:
// - 'w' - write quorum (number, 'quorum', or 'all')
// - 'dw' - durable write quorum (number, 'quorum', or 'all')
// - 'pw' - primary replicas (number, 'quorum', or 'all')
func (c *Client) CreateObject(o *Object, opts map[string]string) error {
	path := "/riak/" + o.Bucket
	req, err := http.NewRequest("POST", c.host+path, o.Body)
	if err != nil {
		return err
	}

	// write content type, links, meta, index stuff
	o.writeheader(req.Header)
	// return info so that we can get vclock, etc.
	query := make(url.Values)
	if opts != nil {
		for key, val := range opts {
			query.Set(key, val)
		}
	}
	query.Set("returnbody", "true")
	req.URL.RawQuery = query.Encode()

	res, err := c.cl.Do(req)
	if err != nil {
		return err
	}
	switch res.StatusCode {
	case 201:
		// this is what we wanted
		loc := res.Header.Get("Location")
		o.Key = strings.TrimPrefix(loc, path+"/")
		return o.fromResponse(res.Header, nil)
	case 400:
		res.Body.Close()
		return ErrBadRequest
	case 404:
		res.Body.Close()
		return ErrNotFound
	case 503:
		res.Body.Close()
		return ErrTimeout
	default:
		res.Body.Close()
		return statusCode(res.StatusCode)
	}

}

// Merge puts an object into the database at /riak/[bucket]/[key]
// Valid opts are:
// - 'w':(number) write quorum
// - 'dw':(number) durable write quorum
// - 'pw':(number) primary replicas
// Merge is successful ONLY if the object in question has not been changed
// since the last read. ErrModified is returned if there has been a change
// since 'o' has been retrieved. You can call c.GetUpdate and then re-try
// the store. Merge will update the object's Vlock and Etag fields.
func (c *Client) Merge(o *Object, opts map[string]string) error {
	//TODO
	req, err := http.NewRequest("PUT", c.host+o.path(), o.Body)
	if err != nil {
		return err
	}
	query := make(url.Values)
	if opts != nil {
		for key, val := range opts {
			query.Set(key, val)
		}
	}
	query.Set("returnbody", "true")
	req.URL.RawQuery = query.Encode()

	o.writeheader(req.Header)
	req.Header.Set("If-Match", o.eTag)
	req.Header.Set("X-Riak-ClientId", c.id)

	res, err := c.cl.Do(req)
	if err != nil {
		return err
	}

	switch res.StatusCode {
	case 200, 201, 204:
		return o.fromResponse(res.Header, res.Body)
	case 400:
		res.Body.Close()
		return ErrBadRequest
	case 300:
		// multiple closes body
		err = multiple(res)
		return err
	case 404:
		res.Body.Close()
		return ErrNotFound
	case 412:
		res.Body.Close()
		return ErrModified
	default:
		res.Body.Close()
		return statusCode(res.StatusCode)
	}
}

// Store stores an object at the object's canonical path (/riak/bucket/key).
// Doesn't do if-not-modified checks. The object's Vclock and Etag fields
// are modified to reflect the server's response.
func (c *Client) Store(o *Object, opts map[string]string) error {
	req, err := http.NewRequest("PUT", c.host+o.path(), o.Body)
	if err != nil {
		return err
	}
	query := make(url.Values)
	if opts != nil {
		for key, val := range opts {
			query.Set(key, val)
		}
	}
	query.Set("returnbody", "true")
	req.URL.RawQuery = query.Encode()

	o.writeheader(req.Header)
	req.Header.Set("X-Riak-ClientId", c.id)

	res, err := c.cl.Do(req)
	if err != nil {
		return err
	}

	switch res.StatusCode {
	case 201, 200, 204:
		// fromresponse closes body
		return o.fromResponse(res.Header, res.Body)
	case 400:
		res.Body.Close()
		return ErrBadRequest
	case 300:
		// multiple closes body
		return multiple(res)
	case 412:
		res.Body.Close()
		return ErrModified
	default:
		res.Body.Close()
		return statusCode(res.StatusCode)
	}
}
