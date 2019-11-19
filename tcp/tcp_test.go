package tcp_test

import (
	"context"
	"testing/quick"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/tcp"
	. "github.com/renproject/aw/testutil"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/renproject/aw/protocol"
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

	Context("rate limiting of tcp server", func() {
		It("should reject connection from client who have attempted to connect too recently", func() {
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
				RateLimit: 3 * time.Second,
			}
			messageReceiver := NewTCPServer(ctx, options, clientSignVerifier)

			// Try to reconnect after sending a message
			_ = sendRandomMessage(messageSender, serverAddr)
			Eventually(messageReceiver, 3*time.Second).Should(Receive())
			clientCancel()

			messageSender = NewTCPClient(ctx, ConnPoolOptions{}, clientSignVerifier)
			_ = sendRandomMessage(messageSender, serverAddr)
			Eventually(messageReceiver).ShouldNot(Receive())

			time.Sleep(3 * time.Second)

			_ = sendRandomMessage(messageSender, serverAddr)
			Eventually(messageReceiver, 3*time.Second).ShouldNot(Receive())
		})
	})
})
