package tcp

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/protocol"
	"github.com/sirupsen/logrus"
)

type ServerOptions struct {
	Logger  logrus.FieldLogger
	Timeout time.Duration
}

type Server struct {
	options    ServerOptions
	messages   protocol.MessageSender
	handshaker handshake.Handshaker
}

func NewServer(options ServerOptions, messages protocol.MessageSender, signVerifier protocol.SignVerifier) *Server {
	return &Server{
		options:    options,
		handshaker: handshake.New(signVerifier),
		messages:   messages,
	}
}

func (server *Server) Listen(ctx context.Context, bind string) error {
	listener, err := net.Listen("tcp", bind)
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		if err := listener.Close(); err != nil {
			server.options.Logger.Errorf("error closing tcp listener: %v", err)
		}
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			// Check whether or not the context is done
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			server.options.Logger.Errorf("error accepting tcp connection: %v", err)
			continue
		}

		// Spawn background goroutine to handle this connection so that it does
		// not block other connections
		go server.handle(ctx, conn)
	}
}

func (server *Server) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	handshakeCtx, handshakeCancel := context.WithTimeout(ctx, server.options.Timeout)
	defer handshakeCancel()
	if err := server.handshaker.AcceptHandshake(handshakeCtx, conn); err != nil {
		server.options.Logger.Errorf("bad handshake with %v: %v", conn.RemoteAddr().String(), err)
		return
	}

	// Accepted connections are not written to, so we prevent write timeouts by
	// setting the write deadline to zero
	conn.SetWriteDeadline(time.Time{})

	// We do not know when the client will send us messages, so we prevent rea
	// timeouts by setting the read deadline to zero
	conn.SetReadDeadline(time.Time{})

	for {
		messageOtw := protocol.MessageOnTheWire{
			From: conn.RemoteAddr(),
		}

		if err := messageOtw.Message.Read(conn); err != nil {
			if err != io.EOF {
				server.options.Logger.Error(newErrReadingIncomingMessage(err))
			}
			return
		}

		select {
		case <-ctx.Done():
			return
		case server.messages <- messageOtw:
		}
	}
}

type ErrReadingIncomingMessage struct {
	error
}

func newErrReadingIncomingMessage(err error) error {
	return ErrReadingIncomingMessage{fmt.Errorf("error reading incoming message: %v", err)}
}
