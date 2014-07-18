package riak

import (
	"bufio"
	"errors"
	"fmt"
	"net/http"
)

// ErrModified is returned when an if-not-modified precondition fails
var ErrModified = errors.New("modified since last read (412)")

// ErrBadRequest is returned when a request is poorly formed
var ErrBadRequest = errors.New("bad request (400)")

// ErrNotFound is returns when the object could not be located
var ErrNotFound = errors.New("not found (404)")

// ErrTimeout is returned when a query or fetch
// request times out server-side
var ErrTimeout = errors.New("riak request timeout (503)")

// ErrMultipleVclocks is an object returned when
// multiple objects reside at the same bucket/key tuple.
// It contains the vector clocks of each object.
type ErrMultipleVclocks struct {
	Vclocks []string
}

func (e *ErrMultipleVclocks) Error() string {
	return "multiple choices (300)"
}

func multiple(res *http.Response) error {
	rd := bufio.NewReader(res.Body)
	e := new(ErrMultipleVclocks)
	for line, err := rd.ReadString('\n'); err != nil; {
		if line == "Siblings:" {
			continue
		}
		e.Vclocks = append(e.Vclocks, line)
	}
	res.Body.Close()
	return e
}

// ErrStatusCode represents a generic
// HTTP status code
type ErrStatusCode struct {
	Code int
}

func (e ErrStatusCode) Error() string {
	return fmt.Sprintf("Error: Status Code %d", e.Code)
}

func statusCode(d int) ErrStatusCode { return ErrStatusCode{Code: d} }
