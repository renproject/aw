package conn

import (
	"context"
	"net"
	"time"
)

// Listen implements a
func Listen(ctx context.Context, address string, handle func(net.Conn), handleErr func(error), allow Allow) error {
	// Create a TCP listener from given address and return an error if unable to do so
	listener, err := new(net.ListenConfig).Listen(ctx, "tcp", address)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		if err := listener.Close(); err != nil {
			handleErr(err)
		}
	}()

	if handleErr == nil {
		handleErr = func(err error) {}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		conn, err := listener.Accept()
		if err != nil {
			handleErr(err)
			continue
		}

		if allow == nil {
			go func() {
				defer func() {
					if err := conn.Close(); err != nil {
						handleErr(err)
					}
				}()
				handle(conn)
			}()
			continue
		}

		if err, cleanup := allow(conn); err == nil {
			go func() {
				defer func() {
					if err := conn.Close(); err != nil {
						handleErr(err)
					}
				}()
				defer func() {
					if cleanup != nil {
						cleanup()
					}
				}()
				handle(conn)
			}()
			continue
		}
		if err := conn.Close(); err != nil {
			handleErr(err)
		}
	}
}

func Dial(ctx context.Context, address string, handle func(net.Conn), handleErr func(error), timeout func(int) time.Duration) error {
	dialer := new(net.Dialer)

	if handleErr == nil {
		handleErr = func(error) {}
	}

	if handle == nil {
		handle = func(net.Conn) {}
	}

	if timeout == nil {
		timeout = func(int) time.Duration { return time.Second }
	}

	for attempt := 1; ; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		dialCtx, dialCancel := context.WithTimeout(ctx, timeout(attempt))
		conn, err := dialer.DialContext(dialCtx, "tcp", address)
		if err != nil {
			handleErr(err)
			<-dialCtx.Done()
			dialCancel()
			continue
		}
		dialCancel()

		return func() (err error) {
			defer func() {
				err = conn.Close()
			}()
			handle(conn)
			return
		}()
	}
}
