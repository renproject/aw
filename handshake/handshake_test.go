package handshake_test

import (
	"context"
	"io"
	"net"
	"reflect"
	"testing/quick"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/handshake"
	. "github.com/renproject/aw/testutil"

	"github.com/renproject/aw/protocol"
	"github.com/renproject/phi"
)

var _ = Describe("Handshaker", func() {

	handshake := func(ctx context.Context, clientConn, serverConn io.ReadWriter) (protocol.Session, protocol.Session) {
		clientSignVerifier := NewMockSignVerifier()
		serverSignVerifier := NewMockSignVerifier(clientSignVerifier.ID())
		clientSignVerifier.Whitelist(serverSignVerifier.ID())

		clientHandshaker := New(clientSignVerifier, NewGCMSessionManager())
		serverHandshaker := New(serverSignVerifier, NewGCMSessionManager())

		var clientErr, serverError error
		var clientSession, serverSession protocol.Session
		phi.ParBegin(func() {
			clientSession, clientErr = clientHandshaker.Handshake(ctx, clientConn)
		}, func() {
			serverSession, serverError = serverHandshaker.AcceptHandshake(ctx, serverConn)
		})
		Expect(clientErr).NotTo(HaveOccurred())
		Expect(serverError).NotTo(HaveOccurred())

		return clientSession, serverSession
	}

	Context("when both client and server are honest", func() {
		Context("when handshaking", func() {
			It("should authenticate the client and server", func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()

				clientConn, serverConn := net.Pipe()
				_, _ = handshake(ctx, clientConn, serverConn)
			})

			It("should cipher and decipher messages", func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()

				clientConn, serverConn := net.Pipe()
				clientSession, serverSession := handshake(ctx, clientConn, serverConn)

				test := func() bool {
					var clientErr, serverError error
					var readMessage protocol.MessageOnTheWire
					message := RandomMessage(protocol.V1, RandomMessageVariant())

					phi.ParBegin(func() {
						clientErr = clientSession.WriteMessage(clientConn, message)
					}, func() {
						readMessage, serverError = serverSession.ReadMessageOnTheWire(serverConn)
					})

					Expect(clientErr).NotTo(HaveOccurred())
					Expect(serverError).NotTo(HaveOccurred())
					return reflect.DeepEqual(readMessage.Message, message)
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})
	})

	PContext("when client is dishonest and server is honest", func() {
		Context("when the client sends a malformed rsa.PublicKey", func() {
			It("should return an error", func() {
				Expect(true).To(BeFalse())
			})
		})

		Context("when the client sends an invalid signature for their rsa.PublicKey", func() {
			It("should return an error", func() {
				Expect(true).To(BeFalse())
			})
		})

		Context("when the client sends an invalid signature for the server session key", func() {
			It("should return an error", func() {
				Expect(true).To(BeFalse())
			})
		})

		Context("when the client sends a valid signature for the server session key that does not match the signature for the client rsa.PublicKey", func() {
			It("should return an error", func() {
				Expect(true).To(BeFalse())
			})
		})

		Context("when the client sends an incorrectly encrypted session key", func() {
			It("should return an error", func() {
				Expect(true).To(BeFalse())
			})
		})

		Context("when the client sends an invalid signature for the client session key", func() {
			It("should return an error", func() {
				Expect(true).To(BeFalse())
			})
		})

		Context("when the client sends a valid signature for the client session key that does not match the signature for the client rsa.PublicKey", func() {
			It("should return an error", func() {
				Expect(true).To(BeFalse())
			})
		})
	})

	PContext("when client is honest and server is dishonest", func() {
		Context("when the server sends a malformed rsa.PublicKey", func() {
			It("should return an error", func() {
				Expect(true).To(BeFalse())
			})
		})

		Context("when the server sends an incorrectly encrypted session key", func() {
			It("should return an error", func() {
				Expect(true).To(BeFalse())
			})
		})

		Context("when the server sends an invalid session key", func() {
			It("should return an error", func() {
				Expect(true).To(BeFalse())
			})
		})

		Context("when the server sends an invalid signature for the server session key", func() {
			It("should return an error", func() {
				Expect(true).To(BeFalse())
			})
		})

		Context("when the server sends an invalid signature for the server rsa.PublicKey", func() {
			It("should return an error", func() {
				Expect(true).To(BeFalse())
			})
		})

		Context("when the server sends a valid signaturefor the server rsa.PublicKey that does not match the signature for the server session key", func() {
			It("should return an error", func() {
				Expect(true).To(BeFalse())
			})
		})

		Context("when the server sends an invalid signature for the client session key", func() {
			It("should return an error", func() {
				Expect(true).To(BeFalse())
			})
		})

		Context("when the server sends a valid signature for the client session key that does not match the signature for the server session key", func() {
			It("should return an error", func() {
				Expect(true).To(BeFalse())
			})
		})
	})
})
