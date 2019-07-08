package tcp

import (
	"context"
	"net"
	"time"

	"github.com/renproject/aw/protocol"
	"github.com/sirupsen/logrus"
)

type ServerOptions struct {
	Logger  logrus.FieldLogger
	Timeout time.Duration
}

type Server struct {
	options  ServerOptions
	messages protocol.MessageSender
}

func NewServer(options ServerOptions, messages protocol.MessageSender) *Server {
	return &Server{
		options:  options,
		messages: messages,
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
			server.options.Logger.Errorf("error accepting tcp connection: %v", err)
			continue
		}
		go server.handle(ctx, conn)
	}
	return nil
}

func (server *Server) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	// Accepted connections are not written to, so we prevent write timeouts by
	// setting the write deadline to zero
	conn.SetWriteDeadline(time.Time{})

	for {
		conn.SetReadDeadline(time.Now().Add(server.options.Timeout))

		message := protocol.Message{}

		if err := message.Read(conn); err != nil {
			// FIXME: handle error
			panic("unimplemented")
		}

		// TODO: Support different versions of messages when there are new
		// versions available.

		messageWire := protocol.MessageOnTheWire{
			From:    conn.RemoteAddr(),
			Message: message,
		}

		select {
		case <-ctx.Done():
			return
		case server.messages <- messageWire:
		}
	}
}
