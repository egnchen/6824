package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = false

// reset timer to duration(not thread-safe)
func SafeResetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		<-t.C
	}
	t.Reset(d)
}

// NotifyChan perform cond.Notify() through channel in a non-blocking manner
func NotifyChan(c chan struct{}) {
	select {
	case c <- struct{}{}:
	default:
	}
}

// DrainChan clear the content of the channel
func DrainChan(c chan struct{}) {
	for {
		select {
		case <-c:
		default:
			return
		}
	}
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Assert(invariant bool) {
	if !invariant {
		panic("Assertion failed")
	}
}
