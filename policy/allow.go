package policy

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"

	"golang.org/x/time/rate"
)

// ErrRateLimited is returned when a connection is dropped because it has
// exceeded its rate limit for connection attempts.
var ErrRateLimited = errors.New("rate limited")

// ErrMaxConnectionsExceeded is returned when a connection is dropped
// because the maximum number of inbound/outbound connections has been
// reached.
var ErrMaxConnectionsExceeded = errors.New("max connections exceeded")

// Allow is a function that filters connections. If an error is returned, the
// connection is filtered and closed. Otherwise, it is maintained. A clean-up
// function is also returned. This function is called after the connection is
// closed, regardless of whether the closure was caused by filtering or normal
// control-flow.
type Allow func(net.Conn) (error, Cleanup)

// Cleanup resource allocation, or reverse per-connection state mutations, done
// by an Allow function.
type Cleanup func()

// All returns an Allow function that only passes a connection if all Allow
// functions in a set pass for that connection. Execution is lazy; when one of
// the Allow functions returns an error, no more Allow functions will be called.
func All(fs ...Allow) Allow {
	return func(conn net.Conn) (error, Cleanup) {
		cleanup := func() {}
		for _, f := range fs {
			err, cleanupF := f(conn)
			if cleanupF != nil {
				cleanupCopy := cleanup
				cleanup = func() {
					cleanupF()
					cleanupCopy()
				}
			}
			if err != nil {
				return err, cleanup
			}
		}
		return nil, cleanup
	}
}

// Any returns an Allow function that passes a connection if any Allow functions
// in a set pass for that connection. Execution is not lazy; even when one of
// the Allow functions returns a non-nil error, all other Allow functions will
// be called.
func Any(fs ...Allow) Allow {
	return func(conn net.Conn) (error, Cleanup) {
		cleanup := func() {}
		any := false
		errs := make([]string, 0, len(fs))
		for _, f := range fs {
			err, cleanupF := f(conn)
			if cleanupF != nil {
				cleanupCopy := cleanup
				cleanup = func() {
					cleanupF()
					cleanupCopy()
				}
			}
			if err == nil {
				any = true
				continue
			}
			errs = append(errs, err.Error())
		}
		if any {
			return nil, cleanup
		}
		return fmt.Errorf("%v", strings.Join(errs, ", ")), cleanup
	}
}

// RateLimit returns an Allow function that rejects an IP-address if it attempts
// too many connections too quickly.
func RateLimit(r rate.Limit, b, cap int) Allow {
	cap /= 2
	front := make(map[string]*rate.Limiter, cap)
	back := make(map[string]*rate.Limiter, cap)

	return func(conn net.Conn) (error, Cleanup) {
		remoteAddr := ""
		if tcpAddr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
			remoteAddr = tcpAddr.IP.String()
		} else {
			remoteAddr = conn.RemoteAddr().String()
		}

		allow := func(limiter *rate.Limiter) (error, func()) {
			if limiter.Allow() {
				return nil, nil
			}
			return ErrRateLimited, nil
		}

		limiter := front[remoteAddr]
		if limiter != nil {
			return allow(limiter)
		}

		limiter = back[remoteAddr]
		if limiter != nil {
			return allow(limiter)
		}

		if len(front) == cap {
			back = front
			front = make(map[string]*rate.Limiter, cap)
		}

		limiter = rate.NewLimiter(r, b)
		front[remoteAddr] = limiter
		return allow(limiter)
	}
}

// Max returns an Allow function that rejects connections once a maximum number
// of connections have already been accepted and are being kept-alive. Once an
// accepted connection is closed, it opens up room for another connection to be
// accepted.
func Max(maxConns int) Allow {
	connsMu := new(sync.RWMutex)
	conns := 0

	return func(conn net.Conn) (error, Cleanup) {
		if maxConns < 0 {
			return nil, nil
		}

		// Pre-emptive read-lock check.
		connsMu.RLock()
		allow := conns < maxConns
		connsMu.RUnlock()
		if !allow {
			return ErrMaxConnectionsExceeded, nil
		}

		// Concurrent-safe write-lock check.
		connsMu.Lock()
		if conns < maxConns {
			conns++
		} else {
			allow = false
		}
		connsMu.Unlock()
		if !allow {
			return ErrMaxConnectionsExceeded, nil
		}

		return nil, func() {
			connsMu.Lock()
			conns--
			connsMu.Unlock()
		}
	}
}
