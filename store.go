package riak

import (
	"net/http"
	"net/url"
)

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
	req, err := http.NewRequest("PUT", o.path(), o.Body)
	if err != nil {
		return err
	}
	var query url.Values
	if opts != nil {
		for key, val := range opts {
			query.Set(key, val)
		}
	}
	query.Set("returnbody", "false")
	req.URL.RawQuery = query.Encode()

	o.writeheader(req.Header)
	req.Header.Set("If-Match", o.eTag)

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

// Store stores a new object at 'path'.
func (c *Client) Store(o *Object, opts map[string]string) error {
	req, err := http.NewRequest("PUT", o.path(), o.Body)
	if err != nil {
		return err
	}
	var query url.Values
	if opts != nil {
		for key, val := range opts {
			query.Set(key, val)
		}
	}
	query.Set("returnbody", "true")
	req.URL.RawQuery = query.Encode()

	o.writeheader(req.Header)

	res, err := c.cl.Do(req)
	if err != nil {
		return err
	}

	switch res.StatusCode {
	case 201, 200, 204:
		err = o.fromResponse(res)
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
