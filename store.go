package riak

import (
	"errors"
	"net/url"
)

var ErrModified = errors.New("Modified since last read.")

// Store puts an object into the database at /buckets/bucket/keys/key
// Valid opts are:
// - 'w':(number) write quorum
// - 'dw':(number) durable write quorum
// - 'pw':(number) primary replicas
// Store is successful ONLY if the object in question has not been changed
// since the last read. ErrModified is returned if there has been a change
// since 'o' has been retrieved.
func (c *Client) Store(o *Object, opts map[string]string) error {
	//TODO
	req, err := c.req("PUT", o.path(), o.Body)
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
	req.htr.URL.RawQuery = query.Encode()

	for key, val := range o.header() {
		req.htr.Header.Set(key, val)
	}
	req.htr.Header.Set("If-Match", "")
	req.htr.Header.Set("If-Unmodified-Since", "")

	res, err := c.doreq(req)
	if err != nil {
		return err
	}

	switch res.StatusCode {
	case 201, 200, 204:
		res.Body.Close()
		return nil
	case 400:
		res.Body.Close()
		return errors.New("Bad Request")
	case 300:
		err = multiple(res)
		res.Body.Close()
		return err
	case 412:
		return ErrModified
	default:
		return errors.New("Bad response.")
	}
}

// Create stores a new object at 'path'. Create is only successful
// if the object doesn't already exist. The objects header values
// (vclock, last-modified, ETag, etc.) will be overwritten by whatever
// Riak returns.
func (c *Client) Create(o *Object, opts map[string]string) error {

	return nil
}
