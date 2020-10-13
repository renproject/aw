package conn

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"

	"golang.org/x/time/rate"
)

var (
	ErrRateLimited            = errors.New("rate limited")
	ErrMaxConnectionsExceeded = errors.New("max connections exceeded")
)

type Allow func(net.Conn) (error, Cleanup)

type Cleanup func()

func All(fs ...Allow) Allow {
	return func(conn net.Conn) (error, Cleanup) {
		cleanup := func() {}
		for _, f := range fs {
			err, cleanupF := f(conn)
			if cleanupF != nil {
				cleanupP := cleanup
				cleanup = func() {
					cleanupF()
					cleanupP()
				}
			}
			if err != nil {
				return err, cleanup
			}
		}
		return nil, cleanup
	}
}

func Any(fs ...Allow) Allow {
	return func(conn net.Conn) (error, Cleanup) {
		cleanup := func() {}
		any := false
		errs := make([]string, 0, len(fs))
		for _, f := range fs {
			err, cleanupF := f(conn)
			if cleanupF != nil {
				cleanupP := cleanup
				cleanup = func() {
					cleanupF()
					cleanupP()
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
