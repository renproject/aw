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
	"github.com/sirupsen/logrus"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/protocol"
)

var _ = Describe("Connection pool", func() {

	Context("when initializing a ConnPool", func() {
		It("should set the options to default if not provided", func() {
			handshaker := handshake.New(NewMockSignVerifier(), handshake.NewGCMSessionManager())
			_ = NewConnPool(logrus.New(), ConnPoolOptions{}, handshaker)
		})

		It("should panic if providing a nil Hanshaker", func() {
			Expect(func() {
				_ = NewConnPool(logrus.New(), ConnPoolOptions{}, nil)
			}).Should(Panic())
		})
	})

	Context("when sending message through the connPool", func() {
		Context("when sending to an address for the first time", func() {
			It("should try to connect to it and maintain the connection in the pool", func() {
				test := func() bool {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer func() {
						cancel()
						time.Sleep(10 * time.Millisecond)
					}()

					// Initialize a connPool
					clientSignVerifier := NewMockSignVerifier()
					handshaker := handshake.New(clientSignVerifier, handshake.NewGCMSessionManager())
					pool := NewConnPool(logrus.New(), ConnPoolOptions{}, handshaker)

					// Initialize a server
					serverAddr, err := net.ResolveTCPAddr("tcp", ":8080")
					Expect(err).NotTo(HaveOccurred())
					options := ServerOptions{Host: serverAddr.String()}
					messages := NewTCPServer(ctx, options, clientSignVerifier)

					// Send 20 messages through the connPool and expect the server receives all of them.
					for i := 0; i < 20; i++ {
						message := RandomMessage(protocol.V1, RandomMessageVariant())
						Expect(pool.Send(serverAddr, message)).NotTo(HaveOccurred())
						var received protocol.MessageOnTheWire
						Eventually(messages, 3*time.Second).Should(Receive(&received))
						Expect(cmp.Equal(message, received.Message, cmpopts.EquateEmpty())).Should(BeTrue())
					}
					return true
				}

				Expect(quick.Check(test, &quick.Config{MaxCount: 10})).NotTo(HaveOccurred())
			})
		})

		Context("when reaching max connection limit", func() {
			It("return an error when trying to send messages to new receiver", func() {
				test := func() bool {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer func() {
						cancel()
						time.Sleep(10 * time.Millisecond)
					}()

					// Initialize a connPool
					clientSignVerifier := NewMockSignVerifier()
					handshaker := handshake.New(clientSignVerifier, handshake.NewGCMSessionManager())
					pool := NewConnPool(logrus.New(), ConnPoolOptions{MaxConnections: 1}, handshaker)

					// Initialize two servers
					serverAddr1, err := net.ResolveTCPAddr("tcp", ":8080")
					Expect(err).NotTo(HaveOccurred())
					serverAddr2, err := net.ResolveTCPAddr("tcp", ":9090")
					Expect(err).NotTo(HaveOccurred())

					_ = NewTCPServer(ctx, ServerOptions{Host: serverAddr1.String()}, clientSignVerifier)
					_ = NewTCPServer(ctx, ServerOptions{Host: serverAddr2.String()}, clientSignVerifier)

					// Expect the second send operation failing due to reaching max number of connections
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
					defer func() {
						cancel()
						time.Sleep(10 * time.Millisecond)
					}()

					// Initialize a connPool
					clientSignVerifier := NewMockSignVerifier()
					handshaker := handshake.New(clientSignVerifier, handshake.NewGCMSessionManager())
					pool := NewConnPool(logrus.New(), ConnPoolOptions{TimeToLive: 100 * time.Millisecond}, handshaker)

					// Initialize a server
					serverAddr, err := net.ResolveTCPAddr("tcp", ":8080")
					Expect(err).NotTo(HaveOccurred())
					options := ServerOptions{Host: serverAddr.String(), RateLimit: -1} // no rate limiting on server
					messages := NewTCPServer(ctx, options, clientSignVerifier)

					// Send a message through the connPool and expect the server receives it.
					message1 := RandomMessage(protocol.V1, RandomMessageVariant())
					Expect(pool.Send(serverAddr, message1)).NotTo(HaveOccurred())
					var received1 protocol.MessageOnTheWire
					Eventually(messages, 3*time.Second).Should(Receive(&received1))
					Expect(cmp.Equal(message1, received1.Message, cmpopts.EquateEmpty()))

					time.Sleep(200 * time.Millisecond)

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

	Context("when trying to connect to a malicious server", func() {
		Context("when server doesn't respond in time", func() {
			It("should timeout the handshake process", func() {
				test := func() bool {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer func() {
						cancel()
						time.Sleep(10 * time.Millisecond)
					}()

					// Initialize a connPool
					clientSignVerifier := NewMockSignVerifier()
					handshaker := handshake.New(clientSignVerifier, handshake.NewGCMSessionManager())
					pool := NewConnPool(logrus.New(), ConnPoolOptions{Timeout: 200 * time.Millisecond}, handshaker)

					// Initialize a server
					serverAddr, err := net.ResolveTCPAddr("tcp", ":8080")
					Expect(err).NotTo(HaveOccurred())
					options := ServerOptions{Host: serverAddr.String(), RateLimit: -1} // no rate limiting on server
					NewMaliciousTCPServer(ctx, options, clientSignVerifier)

					// Send a message through the connPool and expect the server receives it.
					message := RandomMessage(protocol.V1, RandomMessageVariant())
					Expect(pool.Send(serverAddr, message)).To(HaveOccurred())
					return true
				}

				Expect(quick.Check(test, &quick.Config{MaxCount: 10})).NotTo(HaveOccurred())
			})
		})
	})
})
