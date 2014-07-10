package riak

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// GetBuckets gets a list of the buckets
func (c *Client) GetBuckets() ([]string, error) {
	res, err := c.do("GET", "/buckets?buckets=true", nil)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		res.Body.Close()
		return nil, fmt.Errorf("Status Code %d", res.StatusCode)
	}

	bmap := make(map[string][]string)
	dec := json.NewDecoder(res.Body)
	err = dec.Decode(&bmap)
	res.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("Error decoding body: %s", err.Error())
	}

	strs, ok := bmap["buckets"]
	if !ok {
		return nil, errors.New("Unexpected body formatting.")
	}
	return strs, nil
}

// List keys gets all the keys (note: naive)
func (c *Client) ListBucketKeys(bucket string) ([]string, error) {
	res, err := c.do("GET", "/buckets/"+bucket+"/keys?keys=true", nil)
	if err != nil {
		return nil, err
	}
	if res.StatusCode != 200 {
		res.Body.Close()
		return nil, fmt.Errorf("Status Code %d", res.StatusCode)
	}
	bmap := make(map[string][]string)
	dec := json.NewDecoder(res.Body)
	err = dec.Decode(&bmap)
	res.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("Error decoding body: %s", err.Error())
	}
	strs, ok := bmap["keys"]
	if !ok {
		return nil, errors.New("Unexpected body formatting.")
	}
	return strs, nil
}

// BucketProps are the properties of a bucket
type BucketProps struct {
	Name       string `json:"name"`
	Nval       int    `json:"n_val"`
	Mult       bool   `json:"allow_mult"`
	LWW        bool   `json:"last_write_wins"`
	Precommit  []Hook `json:"precommit"`
	Postcommit []Hook `json:"postcommit"`
	HashKey    struct {
		Mod string `json:"mod"`
		Fun string `json:"fun"`
	} `json:"chash_keyfun"`
	Link struct {
		Mod string `json:"mod"`
		Fun string `json:"mapreduce_linkfun"`
	} `json:"linkfun"`
	OldV   int    `json:"old_vclock"`
	YoungV int    `json:"young_vclock"`
	BigV   int    `json:"big_vclock"`
	SmallV int    `json:"small_vclock"`
	R      string `json:"r"`
	W      string `json:"w"`
	DW     string `json:"dw"`
}

type Hook map[string]string

type bucketprops struct {
	b *BucketProps `json:"props"`
}

func (c *Client) GetBucketProps(bucket string) (*BucketProps, error) {
	res, err := c.do("GET", "/buckets/"+bucket+"/props", nil)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != 200 {
		return nil, fmt.Errorf("Status Code %d", res.StatusCode)
	}

	bckts := new(bucketprops)
	dec := json.NewDecoder(res.Body)
	err = dec.Decode(bckts)
	return bckts.b, err
}

func (c *Client) SetBucketProps(bucket string, props *BucketProps) error {
	buf := bytes.NewBuffer(nil)
	enc := json.NewEncoder(buf)
	err := enc.Encode(bucketprops{b: props})
	if err != nil {
		return err
	}

	r, err := c.req("PUT", "/buckets/"+bucket+"/props", buf)
	if err != nil {
		return err
	}

	r.htr.Header.Set("Content-Type", "application/json")
	res, err := c.doreq(r)
	if res.Body != nil {
		res.Body.Close()
	}
	if err != nil {
		return err
	}
	switch res.StatusCode {
	//success
	case 204:
		return nil
		//otherwise
	default:
		return fmt.Errorf("Status Code %d", res.StatusCode)
	}
}

func (c *Client) ResetBucketProps(bucket string) error {
	res, err := c.do("DELETE", "/buckets/"+bucket+"/props", nil)
	if res.Body != nil {
		res.Body.Close()
	}
	if err != nil {
		return err
	}
	switch res.StatusCode {
	case 204:
		return nil
	default:
		return fmt.Errorf("Status Code %d", res.StatusCode)
	}
}
