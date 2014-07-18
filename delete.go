package riak

import (
	"net/http"
)

// Delete removes an object from the database
func (c *Client) Delete(o *Object, opts map[string]string) error {
	if o.Key == "" || o.Bucket == "" {
		return ErrNotFound
	}
	req, err := http.NewRequest("DELETE", c.host+o.path(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-Riak-ClientId", c.id)
	req.Header.Set("X-Riak-Vclock", o.Vclock)
	res, err := c.cl.Do(req)
	if err != nil {
		return err
	}
	res.Body.Close()
	switch res.StatusCode {
	case 204:
		return nil
	case 404:
		return ErrNotFound
	case 400:
		return ErrBadRequest
	default:
		return statusCode(res.StatusCode)
	}

}
