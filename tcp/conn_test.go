package tcp_test

import (
	"context"
	"net"
	"testing/quick"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/tcp"
	. "github.com/renproject/aw/testutil"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/protocol"
)

var _ = Describe("Connection pool", func() {

	Context("when initializing a ConnPool", func() {
		It("should set the options to default if not provided", func() {
			handshaker := handshake.New(NewMockSignVerifier(), handshake.NewGCMSessionManager())
			_ = NewConnPool(ConnPoolOptions{}, handshaker)
		})

		It("should panic if providing a nil Hanshaker", func() {
			Expect(func() {
				_ = NewConnPool(ConnPoolOptions{}, nil)
			}).Should(Panic())
		})
	})

	Context("when sending message through the connPool", func() {
		Context("when sending an address for the first time", func() {
			It("should try to connect to it and maintain the connection in the pool", func() {
				test := func() bool {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()

					// Initialize a connPool
					clientSignVerifier := NewMockSignVerifier()
					handshaker := handshake.New(clientSignVerifier, handshake.NewGCMSessionManager())
					pool := NewConnPool(ConnPoolOptions{}, handshaker)

					// Initialize a server
					serverAddr, err := net.ResolveTCPAddr("tcp", ":8080")
					Expect(err).NotTo(HaveOccurred())
					options := ServerOptions{Host: serverAddr.String()}
					messages := NewTCPServer(ctx, options, clientSignVerifier)

					// Send a message through the connPool and expect the server receives it.
					message := RandomMessage(protocol.V1, RandomMessageVariant())
					Expect(pool.Send(serverAddr, message)).NotTo(HaveOccurred())
					var received protocol.MessageOnTheWire
					Eventually(messages, 3*time.Second).Should(Receive(&received))
					return cmp.Equal(message, received.Message, cmpopts.EquateEmpty())
				}

				Expect(quick.Check(test, &quick.Config{MaxCount: 10})).NotTo(HaveOccurred())
			})
		})

		Context("when reaching max connection limit", func() {
			It("return an error when trying to send messages to new receiver", func() {
				test := func() bool {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()

					// Initialize a connPool
					clientSignVerifier := NewMockSignVerifier()
					handshaker := handshake.New(clientSignVerifier, handshake.NewGCMSessionManager())
					pool := NewConnPool(ConnPoolOptions{MaxConnections: 1}, handshaker)

					// Initialize two servers
					serverAddr1, err := net.ResolveTCPAddr("tcp", ":8080")
					Expect(err).NotTo(HaveOccurred())
					serverAddr2, err := net.ResolveTCPAddr("tcp", ":9090")
					Expect(err).NotTo(HaveOccurred())

					_ = NewTCPServer(ctx, ServerOptions{Host: serverAddr1.String()}, clientSignVerifier)
					_ = NewTCPServer(ctx, ServerOptions{Host: serverAddr2.String()}, clientSignVerifier)

					// Send a message through the connPool and expect the server receives it.
					message := RandomMessage(protocol.V1, RandomMessageVariant())
					Expect(pool.Send(serverAddr1, message)).NotTo(HaveOccurred())
					Expect(pool.Send(serverAddr2, message)).To(HaveOccurred())
					return true
				}

				Expect(quick.Check(test, &quick.Config{MaxCount: 10})).NotTo(HaveOccurred())
			})
		})

		Context("when the connection has been open more than TimeToLive time", func() {
			It("should close the connection to release the resources", func() {
				test := func() bool {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()

					// Initialize a connPool
					clientSignVerifier := NewMockSignVerifier()
					handshaker := handshake.New(clientSignVerifier, handshake.NewGCMSessionManager())
					pool := NewConnPool(ConnPoolOptions{}, handshaker)

					// Initialize a server
					serverAddr, err := net.ResolveTCPAddr("tcp", ":8080")
					Expect(err).NotTo(HaveOccurred())
					options := ServerOptions{Host: serverAddr.String()}
					messages := NewTCPServer(ctx, options, clientSignVerifier)

					// Send a message through the connPool and expect the server receives it.
					message1 := RandomMessage(protocol.V1, RandomMessageVariant())
					Expect(pool.Send(serverAddr, message1)).NotTo(HaveOccurred())
					var received1 protocol.MessageOnTheWire
					Eventually(messages, 3*time.Second).Should(Receive(&received1))
					Expect(cmp.Equal(message1, received1.Message, cmpopts.EquateEmpty()))

					time.Sleep(time.Second)

					// Expect the connPool drop the previous session and try create a new one
					message2 := RandomMessage(protocol.V1, RandomMessageVariant())
					Expect(pool.Send(serverAddr, message2)).NotTo(HaveOccurred())
					var received2 protocol.MessageOnTheWire
					Eventually(messages, 3*time.Second).Should(Receive(&received2))
					Expect(cmp.Equal(message2, received2.Message, cmpopts.EquateEmpty()))

					return true
				}

				Expect(quick.Check(test, &quick.Config{MaxCount: 10})).NotTo(HaveOccurred())
			})
		})
	})
})
