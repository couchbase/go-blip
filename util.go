package blip

import (
	"sync/atomic"
	"time"
)

// waitForZeroActiveGoroutines blocks until either the number of activeGoroutines has reached zero, or we give up waiting.
func waitForZeroActiveGoroutines(logCtx *Context, activeGoroutines *int32) {
	timeout := time.After(time.Second * 5)
	ticker := time.NewTicker(time.Millisecond * 25)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			logCtx.log("timed out waiting for goroutines to finish")
			return // timed out
		case <-ticker.C:
			if atomic.LoadInt32(activeGoroutines) > 0 {
				continue
			}
			return // all done
		}
	}
}

// errorFromChannel returns an error if there's one in the given channel, otherwise returns nil.
func errorFromChannel(c chan error) error {
	select {
	case err := <-c:
		if err != nil {
			return err
		}
	default:
	}
	return nil
}
