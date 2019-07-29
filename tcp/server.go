package tcp

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/protocol"
	"github.com/sirupsen/logrus"
)

type ServerOptions struct {
	Logger       logrus.FieldLogger
	Timeout      time.Duration
	SignVerifier protocol.SignVerifier
}

type Server struct {
	options    ServerOptions
	messages   protocol.MessageSender
	handShaker handshake.HandShaker
}

func NewServer(options ServerOptions, messages protocol.MessageSender) *Server {
	return &Server{
		options:    options,
		handShaker: handshake.NewHandShaker(options.SignVerifier),
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
		// Check whether or not the context is done
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		conn, err := listener.Accept()
		if err != nil {
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

	handshakeCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := server.handShaker.Respond(handshakeCtx, conn); err != nil {
		server.options.Logger.Errorf("failed to perform handshake with %s: %v", conn.RemoteAddr().String(), err)
		return
	}

	// Accepted connections are not written to, so we prevent write timeouts by
	// setting the write deadline to zero
	conn.SetWriteDeadline(time.Time{})

	// We do not know when the client will send us messages, so we prevent rea timeouts by setting the read deadline to zero
	conn.SetReadDeadline(time.Time{})

	for {
		messageOtw := protocol.MessageOnTheWire{
			From: conn.RemoteAddr(),
		}

		if err := messageOtw.Message.Read(conn); err != nil {
			server.options.Logger.Error(newErrReadingIncomingMessage(err))
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
