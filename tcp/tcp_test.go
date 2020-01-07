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
	"github.com/renproject/aw/protocol"
	"github.com/sirupsen/logrus"
)

var _ = Describe("TCP client and server", func() {

	sendRandomMessage := func(messageSender protocol.MessageSender, to protocol.PeerAddress) protocol.Message {
		message := RandomMessage(protocol.V1, RandomMessageVariant())
		messageOtw := protocol.MessageOnTheWire{
			To:      to,
			Message: message,
		}
		messageSender <- messageOtw
		return message
	}

	BeforeEach(func() {
		// Give enough time for server to clean up between tests
		time.Sleep(500 * time.Millisecond)
	})

	Context("when initializing a server", func() {
		It("should panic if providing a nil handshaker", func() {
			Expect(func() {
				_ = NewServer(ServerOptions{}, logrus.New(), nil)
			}).Should(Panic())
		})
	})

	Context("when sending a message", func() {
		It("should create a session between client and server and successfully send the message through the session", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Initialize a client
			clientSignVerifier := NewMockSignVerifier()
			messageSender := NewTCPClient(ctx, ConnPoolOptions{}, clientSignVerifier)

			// Initialize a server
			serverAddr := NewSimpleTCPPeerAddress(RandomPeerID().String(), "", "8080")
			options := ServerOptions{Host: serverAddr.NetworkAddress().String()}
			messageReceiver := NewTCPServer(ctx, options, clientSignVerifier)

			// Send a message through the messageSender and expect the server receives it.
			test := func() bool {
				message := sendRandomMessage(messageSender, serverAddr)
				var received protocol.MessageOnTheWire
				Eventually(messageReceiver, 3*time.Second).Should(Receive(&received))
				return cmp.Equal(message, received.Message, cmpopts.EquateEmpty())
			}

			Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
		})
	})

	Context("when reach max number of connection allowed", func() {
		It("show reject the connection", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Initialize a server
			serverAddr := NewSimpleTCPPeerAddress(RandomPeerID().String(), "", "8080")
			options := ServerOptions{Host: serverAddr.NetworkAddress().String(), MaxConnections: 1}
			_ = NewTCPServer(ctx, options)

			addr1, err := net.ResolveTCPAddr("tcp", ":10000")
			Expect(err).NotTo(HaveOccurred())
			dialer1 := net.Dialer{LocalAddr: addr1}
			_, err = dialer1.Dial("tcp", ":8080")
			Expect(err).NotTo(HaveOccurred())

			addr2, err := net.ResolveTCPAddr("tcp", ":10001")
			Expect(err).NotTo(HaveOccurred())
			dialer2 := net.Dialer{LocalAddr: addr2}
			conn, err := dialer2.Dial("tcp", ":8080")
			Expect(err).NotTo(HaveOccurred())
			_, err = conn.Read(make([]byte, 10))
			Expect(err).To(HaveOccurred())
		})
	})

	Context("rate limiting of tcp server", func() {
		It("should reject connection from client who has attempted to connect too recently", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// Initialize a client
			clientSignVerifier := NewMockSignVerifier()
			clientCtx, clientCancel := context.WithCancel(ctx)
			messageSender := NewTCPClient(clientCtx, ConnPoolOptions{}, clientSignVerifier)

			// Initialize a server
			serverAddr := NewSimpleTCPPeerAddress(RandomPeerID().String(), "", "8080")
			options := ServerOptions{
				Host:      serverAddr.NetworkAddress().String(),
				RateLimit: 2 * time.Second,
			}
			messageReceiver := NewTCPServer(ctx, options, clientSignVerifier)

			// Try to connect and send a message to server
			_ = sendRandomMessage(messageSender, serverAddr)
			Eventually(messageReceiver, 3*time.Second).Should(Receive())

			// Cancel the ctx which close the connection
			time.Sleep(100 * time.Millisecond)
			clientCancel()
			time.Sleep(100 * time.Millisecond)

			// Create a new client and send a message and expect it to be rejected.
			messageSender = NewTCPClient(ctx, ConnPoolOptions{}, clientSignVerifier)
			_ = sendRandomMessage(messageSender, serverAddr)
			Eventually(messageReceiver).ShouldNot(Receive())

			// Wait for the rate limit expire
			time.Sleep(3 * time.Second)

			// Expect the connection to be accepted by the server
			message := sendRandomMessage(messageSender, serverAddr)
			var received protocol.MessageOnTheWire
			Eventually(messageReceiver, time.Second).Should(Receive(&received))
			Expect(cmp.Equal(message, received.Message, cmpopts.EquateEmpty())).Should(BeTrue())
		})
	})

	Context("when an honest server is dialed by a malicious client", func() {
		Context("when client doesn't do anything in the handshake process", func() {
			It("should timeout after sometime", func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				// Initialize a malicious client
				clientSignVerifier := NewMockSignVerifier()
				messageSender := NewMaliciousTCPClient(ctx, ConnPoolOptions{}, clientSignVerifier)

				// Initialize a server
				serverAddr := NewSimpleTCPPeerAddress(RandomPeerID().String(), "", "8080")
				options := ServerOptions{Host: serverAddr.NetworkAddress().String(), Timeout: 500 * time.Millisecond}
				messageReceiver := NewTCPServer(ctx, options, clientSignVerifier)

				// Send a message through the messageSender and expect the server reject the connection.
				_ = sendRandomMessage(messageSender, serverAddr)
				Eventually(messageReceiver, 3*time.Second).ShouldNot(Receive())
			})
		})
	})
})
