package riak

import (
	"bufio"
	"net/url"
)

type ErrMultipleVclocks struct {
	Vclocks []string
}

func (e *ErrMultipleVclocks) Error() {
	return "Error: Multiple Choices"
}

func multiple(res *http.Response) error {
	rd := bufio.NewReader(res.Body)
	e := new(ErrMultipleVclocks)
	for line, err := rb.ReadString("\n"); err != nil; {
		if line == "Siblings:" {
			continue
		}
		e.Vclocks = append(e.Vclocks, line)
	}
	res.Body.Close()
	return e
}

// Fetch gets a riak Object
// Valid options are:
// - 'r':(number) (read quorum)
// - 'pr':(number) (primary replicas)
// - 'basic_quorum':(true/false)
// - 'notfound_ok':(true/false)
// - 'vtag':(vtag)
// Fetch returns ErrMultipleVclocks if multiple options are available.
func (c *Client) Fetch(path string, opts map[string]string) (*Object, error) {
	req, err := c.req("GET", path, nil)
	if err != nil {
		return nil, err
	}

	// url-encode opts
	if opts != nil {
		var query url.Values
		for key, val := range opts {
			query.Set(key, val)
		}
		req.htr.URL.RawQuery = query.Encode()
	}

	res, err := c.doreq(req)
	if err != nil {
		return nil, err
	}
	if res.StatusCode == 300 {
		return nil, multiple(res)
	}
	o := new(Object)
	err = o.fromResponse(res)
	return o, err
}
