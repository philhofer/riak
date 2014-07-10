package riak

import (
	"io"
	"net/http"
	"time"
)

// Object is a Riak object
type Object struct {
	Ctype        *string     //Content-Type
	Vclock       *string     //Vclock
	ETag         *string     //Etag
	LastModified time.Time   //Last-Modified
	Links        []Link      // Link: <>
	Meta         [][2]string // X-Riak-Meta-*
	Index        [][2]string // X-Riak-Index-*
	Body         io.Reader   // Body
}

func (o *Object) fromResponse(res *http.Response) error {
	//TODO
	return nil
}

type Link struct {
	Tagname string
	Bucket  string
	Key     string
}
