package testutil

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/protocol"
	"github.com/renproject/aw/tcp"
	"github.com/renproject/phi"
	"github.com/sirupsen/logrus"
)

func NewConnection(port string) (io.ReadWriter, io.ReadWriter, func() error, error) {
	addr := fmt.Sprintf(":%v", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, nil, err
	}

	var clientConn, serverConn net.Conn
	var clientErr, serverErr error
	phi.ParBegin(func() {
		serverConn, serverErr = listener.Accept()
		if err != nil {
			return
		}
	}, func() {
		clientConn, clientErr = net.Dial("tcp", addr)
		if err != nil {
			return
		}
	})

	stop := func() error {
		if serverConn != nil {
			if err := serverConn.Close(); err != nil {
				return err
			}
		}
		if clientConn != nil {
			if err := clientConn.Close(); err != nil {
				return err
			}
		}

		return nil
	}

	if clientErr != nil || serverErr != nil {
		if err := stop(); err != nil {
			return nil, nil, nil, err
		}
		return nil, nil, nil, fmt.Errorf("client err = %v, server error = %v", clientErr, serverErr)
	}
	return clientConn, serverConn, stop, nil
}

func NewTCPClient(ctx context.Context, options tcp.ConnPoolOptions, verifier protocol.SignVerifier) protocol.MessageSender {
	messages := make(chan protocol.MessageOnTheWire, 128)
	handshaker := handshake.New(verifier, handshake.NewGCMSessionManager())
	client := tcp.NewClient(logrus.StandardLogger(), tcp.NewConnPool(logrus.New(), options, handshaker))

	go client.Run(ctx, messages)
	return messages
}

func NewMaliciousTCPClient(ctx context.Context, options tcp.ConnPoolOptions, verifier protocol.SignVerifier) protocol.MessageSender {
	messages := make(chan protocol.MessageOnTheWire, 128)
	handshaker := NewMalHanshaker(verifier, handshake.NewGCMSessionManager())
	client := tcp.NewClient(logrus.StandardLogger(), tcp.NewConnPool(logrus.New(), options, handshaker))

	go client.Run(ctx, messages)
	return messages
}

func NewTCPServer(ctx context.Context, options tcp.ServerOptions, clientSignVerifiers ...MockSignVerifier) chan protocol.MessageOnTheWire {
	signVerifier := NewMockSignVerifier()
	for _, clientSignVerifier := range clientSignVerifiers {
		signVerifier.Whitelist(clientSignVerifier.ID())
		clientSignVerifier.Whitelist(signVerifier.ID())
	}

	handshaker := handshake.New(signVerifier, handshake.NewGCMSessionManager())
	server := tcp.NewServer(logrus.New(), options, handshaker)
	messageSender := make(chan protocol.MessageOnTheWire, 128)
	go server.Run(ctx, messageSender)
	time.Sleep(50 * time.Millisecond)

	return messageSender
}

func NewMaliciousTCPServer(ctx context.Context, options tcp.ServerOptions, clientSignVerifiers ...MockSignVerifier) {
	signVerifier := NewMockSignVerifier()
	for _, clientSignVerifier := range clientSignVerifiers {
		signVerifier.Whitelist(clientSignVerifier.ID())
		clientSignVerifier.Whitelist(signVerifier.ID())
	}

	handshaker := NewMalHanshaker(signVerifier, handshake.NewGCMSessionManager())
	server := tcp.NewServer(logrus.New(), options, handshaker)
	messageSender := make(chan protocol.MessageOnTheWire, 128)
	go server.Run(ctx, messageSender)
	time.Sleep(50 * time.Millisecond)
}

// MalHanshaker will not do nothing in handshake process but hanging there.
type MalHanshaker struct {
	signVerifier   protocol.SignVerifier
	sessionManager protocol.SessionManager
}

func NewMalHanshaker(signVerifier protocol.SignVerifier, sessionManager protocol.SessionManager) handshake.Handshaker {
	if signVerifier == nil {
		panic("invariant violation: SignVerifier cannot be nil")
	}
	if sessionManager == nil {
		panic("invariant violation: SessionManager cannot be nil")
	}
	return &MalHanshaker{
		signVerifier:   signVerifier,
		sessionManager: sessionManager,
	}
}

func (m *MalHanshaker) Handshake(ctx context.Context, rw io.ReadWriter) (protocol.Session, error) {
	<-ctx.Done()
	return nil, nil
}

func (m *MalHanshaker) AcceptHandshake(ctx context.Context, rw io.ReadWriter) (protocol.Session, error) {
	<-ctx.Done()
	return nil, nil
}
