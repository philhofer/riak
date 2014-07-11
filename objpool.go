package riak

import (
	"bytes"
	"sync"
)

var opool *sync.Pool

func init() {
	opool = new(sync.Pool)
	opool.New = func() interface{} {
		return &Object{
			Body: bytes.NewBuffer(nil),
		}
	}
}

// Release releases an object to this package's
// internal free pool of objects. Releasing objects
// when you are done with them isn't necessary, but
// may help reduce GC activity.
func Release(o *Object) {
	o.hardReset()
	opool.Put(o)
}

func newObj() *Object {
	obj, ok := opool.Get().(*Object)
	if !ok {
		return opool.New().(*Object)
	}
	return obj
}
