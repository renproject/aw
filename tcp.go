package aw

import (
	"context"
	"fmt"
	"net"
	"time"

	"golang.org/x/time/rate"
)

var (
	DefaultRateLimiterCapacity        int                = 10
	DefaultListenerRateLimiterOptions RateLimiterOptions = RateLimiterOptions{Rate: 10, Burst: 20}
	DefaultRate                       rate.Limit         = 1024 * 1024
	DefaultBurst                      int                = 4 * 1024 * 1024
)

type ListenerOptions struct {
	RateLimiterCapacity int
	RateLimiterOptions
}

func DefaultListenerOptions() ListenerOptions {
	return ListenerOptions{
		RateLimiterCapacity: DefaultRateLimiterCapacity,
		RateLimiterOptions:  DefaultListenerRateLimiterOptions,
	}
}

type RateLimiterOptions struct {
	Rate  rate.Limit
	Burst int
}

func DefaultRateLimiterOptions() RateLimiterOptions {
	return RateLimiterOptions{
		Rate:  DefaultRate,
		Burst: DefaultBurst,
	}
}

func dial(ctx context.Context, remoteAddr string, retryInterval time.Duration) (net.Conn, error) {
	dialer := new(net.Dialer)

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("dialing %w", ctx.Err())

		default:
		}

		dialCtx, dialCancel := context.WithTimeout(ctx, retryInterval)
		conn, err := dialer.DialContext(dialCtx, "tcp", remoteAddr)
		if err != nil {
			// TODO(ross): I don't think we care about this error. Do we?
			// Should it be logged?

			<-dialCtx.Done()
			dialCancel()
		} else {
			dialCancel()

			return conn, nil
		}
	}
}

func listen(ctx context.Context, listener net.Listener, handle func(net.Conn), rateLimiterOptions ListenerOptions) {
	mapCap := rateLimiterOptions.RateLimiterCapacity / 2
	frontMap := make(map[string]*rate.Limiter, mapCap)
	backMap := make(map[string]*rate.Limiter, mapCap)

	for {
		select {
		case <-ctx.Done():
			listener.Close()
			return

		default:
		}

		conn, err := listener.Accept()
		if err != nil {
			// TODO(ross): Do we want to do something other than logging here?
		} else {
			// Rate limiting.
			remoteAddr := ""
			if tcpAddr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
				remoteAddr = tcpAddr.IP.String()
			} else {
				remoteAddr = conn.RemoteAddr().String()
			}

			var ok bool
			var limiter *rate.Limiter
			if limiter, ok = frontMap[remoteAddr]; !ok {
				if limiter, ok = backMap[remoteAddr]; !ok {
					if len(frontMap) == mapCap {
						backMap = frontMap
						frontMap = make(map[string]*rate.Limiter, mapCap)
					}

					limiter = rate.NewLimiter(rateLimiterOptions.Rate, rateLimiterOptions.Burst)
					frontMap[remoteAddr] = limiter
				}
			}

			if limiter.Allow() {
				go func() {
					handle(conn)
				}()
			} else {
				conn.Close()
				// TODO(ross): Logging?
			}
		}
	}
}
