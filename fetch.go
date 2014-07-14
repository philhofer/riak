package riak

import (
	"net/http"
	"net/url"
)

// Fetch gets a riak Object
// Valid options are:
// - 'r':(number) (read quorum)
// - 'pr':(number) (primary replicas)
// - 'basic_quorum':(true/false)
// - 'notfound_ok':(true/false)
// - 'vtag':(vtag) - which sibling to retrieve, if multiple siblings
// Fetch returns ErrMultipleVclocks if multiple options are available.
func (c *Client) Fetch(path string, opts map[string]string) (*Object, error) {
	req, err := http.NewRequest("GET", c.host+path, nil)
	if err != nil {
		return nil, err
	}

	// url-encode opts
	if opts != nil {
		var query url.Values
		for key, val := range opts {
			query.Set(key, val)
		}
		req.URL.RawQuery = query.Encode()
	}
	req.Header.Set("X-Riak-ClientId", c.id)

	res, err := c.cl.Do(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode == 300 {
		return nil, multiple(res)
	}
	o := newObj()
	err = o.fromResponse(res.Header, res.Body)
	return o, err
}

// Update checks if the object has been changed, and if it has,
// it overwrites the object and returns 'true'.
func (c *Client) GetUpdate(o *Object, opts map[string]string) (bool, error) {
	req, err := http.NewRequest("GET", c.host+o.path(), nil)
	if err != nil {
		return false, err
	}

	if opts != nil {
		var query url.Values
		for key, val := range opts {
			query.Set(key, val)
		}
		req.URL.RawQuery = query.Encode()
	}

	req.Header.Set("If-None-Match", o.eTag)
	req.Header.Set("X-Riak-ClientId", c.id)

	o.writeheader(req.Header)

	res, err := c.cl.Do(req)
	if err != nil {
		return false, err
	}
	// not modified
	switch res.StatusCode {

	case 304:
		//not modified
		res.Body.Close()
		return false, nil

	case 300:
		return false, multiple(res)

	case 200:
		// modified
		err = o.fromResponse(res.Header, res.Body)
		return true, err

	case 400:
		res.Body.Close()
		return false, ErrBadRequest

	default:
		res.Body.Close()
		return false, statusCode(res.StatusCode)
	}

}
