package main

import (
	"sync"
)

type CB struct {
	sync.Mutex
	p int
	n int
	b chan struct{}
}

func (cb *CB) reset() {
	cb.Lock()
	defer cb.Unlock()

	cb.n = cb.p
	close(cb.b)
	cb.b = make(chan struct{})

}

func (cb *CB) Await() {
	cb.Lock()
	cb.n--
	n := cb.n
	b := cb.b
	cb.Unlock()

	if n > 0 {
		<-b
	} else {
		cb.reset()
	}
}

func NewCB(parties int) *CB {
	cb := &CB{
		p: parties,
		n: parties,
		b: make(chan struct{}),
	}
	return cb
}
