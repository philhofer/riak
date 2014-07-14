package riak

import (
	"bufio"
	"errors"
	"fmt"
	"net/http"
)

var ErrModified = errors.New("Modified since last read.")
var ErrBadRequest = errors.New("Bad Request")

type ErrMultipleVclocks struct {
	Vclocks []string
}

type ErrInvalidBody struct {

}

func (e *ErrMultipleVclocks) Error() string {
	return "Error: Multiple Choices"
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

type ErrStatusCode struct {
	Code int
}

func (e ErrStatusCode) Error() string {
	return fmt.Sprintf("Error: Status Code %d", e.Code)
}

func statusCode(d int) ErrStatusCode { return ErrStatusCode{Code: d} }
