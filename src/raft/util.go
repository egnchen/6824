package raft

import (
	"log"
	"runtime/debug"
	"sync"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func WaitForCond(cond *sync.Cond, lambda func() bool) chan struct{} {
	ch := make(chan struct{})
	go func() {
		cond.L.Lock()
		defer cond.L.Unlock()
		for !lambda() {
			cond.Wait()
		}
		close(ch)
	}()
	return ch
}

func Assert(invariant bool) {
	if !invariant {
		debug.PrintStack()
		panic("Assertion failed")
	}
}
