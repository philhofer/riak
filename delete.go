package riak

import ()

func (c *Client) Delete(o *Object, opts map[string]string) error {
	if o.Key == "" || o.Bucket == "" {
		return ErrNotFound
	}

	res, err := c.do("DELETE", o.path(), nil)
	if err != nil {
		return err
	}
	res.Body.Close()
	switch res.StatusCode {
	case 204:
		return nil
	case 404:
		return ErrNotFound
	default:
		return statusCode(res.StatusCode)
	}
}
