package tcp

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/renproject/aw/policy"
)

// Listen for connections from remote peers until the context is done. The
// allow function will be used to control the acceptance/rejection of connection
// attempts, and can be used to implement maximum connection limits, per-IP
// rate-limiting, and so on. This function spawns all accepted connections into
// their own background goroutines that run the handle function, and then
// clean-up the connection. This function blocks until the context is done.
func Listen(ctx context.Context, address string, handle func(net.Conn), handleErr func(error), allow policy.Allow) error {
	// Create a TCP listener from given address and return an error if unable to do so
	listener, err := new(net.ListenConfig).Listen(ctx, "tcp", address)
	if err != nil {
		return err
	}
	return ListenWithListener(ctx, listener, handle, handleErr, allow)
}

// NOTE: The listener passed to this function will be closed when the given
// context finishes.
func ListenWithListener(ctx context.Context, listener net.Listener, handle func(net.Conn), handleErr func(error), allow policy.Allow) error {
	if handle == nil {
		return fmt.Errorf("nil handle function")
	}

	if handleErr == nil {
		handleErr = func(err error) {}
	}

	go func() {
		<-ctx.Done()
		if err := listener.Close(); err != nil {
			handleErr(fmt.Errorf("close listener: %v", err))
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		conn, err := listener.Accept()
		if err != nil {
			handleErr(fmt.Errorf("accept connection: %v", err))
			continue
		}

		if allow == nil {
			go func() {
				defer func() {
					if err := conn.Close(); err != nil {
						handleErr(fmt.Errorf("close connection: %v", err))
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
						handleErr(fmt.Errorf("close connection: %v", err))
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
			handleErr(fmt.Errorf("close connection: %v", err))
		}
	}
}

func ListenerWithAssignedPort(ctx context.Context, ip net.IP) (net.Listener, int, error) {
	listener, err := new(net.ListenConfig).Listen(ctx, "tcp", fmt.Sprintf("%v:%v", ip.String(), 0))
	if err != nil {
		return nil, 0, err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	return listener, port, nil
}

// Dial a remote peer until a connection is successfully established, or until
// the context is done. Multiple dial attempts can be made, and the timeout
// function is used to define an upper bound on dial attempts. This function
// blocks until the connection is handled (and the handle function returns).
// This function will clean-up the connection.
func Dial(ctx context.Context, address string, handle func(net.Conn), handleErr func(error), timeout func(int) time.Duration) error {
	dialer := new(net.Dialer)

	if handle == nil {
		return fmt.Errorf("nil handle function")
	}

	if handleErr == nil {
		handleErr = func(error) {}
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
