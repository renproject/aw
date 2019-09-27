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

const DefaultTCPPort = 19231

type ServerOptions struct {
	Logger           logrus.FieldLogger
	Timeout          time.Duration
	TimeoutKeepAlive time.Duration
	Port             int
	Handshaker       handshake.Handshaker
}

func (options *ServerOptions) setZerosToDefaults() {
	if options.Logger == nil {
		options.Logger = logrus.New()
	}
	if options.Timeout == 0 {
		options.Timeout = 20 * time.Second
	}
	if options.TimeoutKeepAlive == 0 {
		options.TimeoutKeepAlive = 10 * time.Second
	}
	if options.Port == 0 {
		options.Port = DefaultTCPPort
	}
}

type Server struct {
	options  ServerOptions
	messages protocol.MessageSender
}

func NewServer(options ServerOptions, messages protocol.MessageSender) *Server {
	options.setZerosToDefaults()
	return &Server{
		options:  options,
		messages: messages,
	}
}

func (server *Server) Run(ctx context.Context) {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{
		Port: server.options.Port,
	})
	if err != nil {
		server.options.Logger.Errorf("error listening on: 0.0.0.0:%d: %v", server.options.Port, err)
		return
	}

	go func() {
		<-ctx.Done()
		if err := listener.Close(); err != nil {
			server.options.Logger.Errorf("error closing listener: %v", err)
		}
	}()

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			// Check whether or not the context is done
			select {
			case <-ctx.Done():
				return
			default:
			}

			server.options.Logger.Errorf("error accepting connection: %v", err)
			continue
		}
		conn.SetKeepAlive(true)
		conn.SetKeepAlivePeriod(server.options.Timeout)

		// Spawn background goroutine to handle this connection so that it does
		// not block other connections
		go server.handle(ctx, conn)
	}
}

func (server *Server) handle(ctx context.Context, conn *net.TCPConn) {
	defer conn.Close()

	if server.options.Handshaker != nil {
		handshakeCtx, handshakeCancel := context.WithTimeout(ctx, server.options.Timeout)
		defer handshakeCancel()

		if err := server.options.Handshaker.AcceptHandshake(handshakeCtx, conn); err != nil {
			server.options.Logger.Errorf("bad handshake with %v: %v", conn.RemoteAddr().String(), err)
			return
		}
	}

	for {
		messageOtw := protocol.MessageOnTheWire{}
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
