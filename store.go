package riak

import (
	"net/http"
	"net/url"
	"strings"
)

// CreateObject creates a new object in 'bucket' and modifies the object
// key to be the key that riak assigned it. Only the 'body' and 'bucket'
// fields of the object need to be defined.
func (c *Client) CreateObject(o *Object) error {
	path := "/riak/" + o.Bucket
	req, err := http.NewRequest("POST", c.host+path, o.Body)
	if err != nil {
		return err
	}
	o.writeheader(req.Header)

	res, err := c.cl.Do(req)
	if err != nil {
		return err
	}
	// we never use the body
	res.Body.Close()
	switch res.StatusCode {
	case 201:
		// this is what we wanted
		loc := res.Header.Get("Location")
		o.Key = strings.TrimPrefix(path+"/", loc)
		return nil
	case 400:
		return ErrBadRequest
	default:
		return statusCode(res.StatusCode)
	}

}

// Merge puts an object into the database at /buckets/bucket/keys/key
// Valid opts are:
// - 'w':(number) write quorum
// - 'dw':(number) durable write quorum
// - 'pw':(number) primary replicas
// Merge is successful ONLY if the object in question has not been changed
// since the last read. ErrModified is returned if there has been a change
// since 'o' has been retrieved. You can call c.GetUpdate and then re-try
// the store.
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
	query.Set("returnbody", "false")
	req.URL.RawQuery = query.Encode()

	o.writeheader(req.Header)
	req.Header.Set("If-Match", o.eTag)
	req.Header.Set("X-Riak-ClientId", c.id)

	res, err := c.cl.Do(req)
	if err != nil {
		return err
	}

	switch res.StatusCode {
	case 201, 200, 204:
		// read new vclock
		vclk := res.Header.Get("X-Riak-Vclock")
		if vclk != "" {
			o.Vclock = vclk
		}
		res.Body.Close()
		return nil
	case 400:
		res.Body.Close()
		return ErrBadRequest
	case 300:
		err = multiple(res)
		return err
	case 412:
		return ErrModified
	default:
		return statusCode(res.StatusCode)
	}
}

// Store stores an object at the object's canonical path (/riak/bucket/key).
// Doesn't do if-not-modified checks.
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
	query.Set("returnbody", "false")
	req.URL.RawQuery = query.Encode()

	o.writeheader(req.Header)
	req.Header.Set("X-Riak-ClientId", c.id)

	res, err := c.cl.Do(req)
	if err != nil {
		return err
	}

	switch res.StatusCode {
	case 201, 200, 204:
		err = o.fromResponse(res.Header, res.Body)
		return err
	case 400:
		res.Body.Close()
		return ErrBadRequest
	case 300:
		err = multiple(res)
		res.Body.Close()
		return err
	case 412:
		res.Body.Close()
		return ErrModified
	default:
		res.Body.Close()
		return statusCode(res.StatusCode)
	}
}
