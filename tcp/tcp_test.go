package tcp_test

import (
	"context"
	"reflect"
	"testing/quick"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/tcp"
	. "github.com/renproject/aw/testutil"

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
				return reflect.DeepEqual(message, received.Message)
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

// import (
// 	"bytes"
// 	"context"
// 	"crypto/rand"
// 	"fmt"
// 	"reflect"
// 	"testing/quick"
// 	"time"
//
// 	"github.com/renproject/aw/dht"
// 	"github.com/renproject/aw/handshake"
// 	"github.com/renproject/aw/protocol"
// 	"github.com/renproject/aw/tcp"
// 	"github.com/renproject/aw/testutil"
// 	"github.com/renproject/kv"
// 	"github.com/sirupsen/logrus"
//
// 	. "github.com/onsi/ginkgo"
// 	. "github.com/onsi/gomega"
// )
//
// var _ = Describe("Tcp", func() {
//
// 	initServer := func(ctx context.Context, sender protocol.MessageSender, hs handshake.Handshaker, host string) {
// 		tcp.NewServer(tcp.ServerOptions{
// 			Logger:     logrus.StandardLogger(),
// 			Timeout:    time.Minute,
// 			Handshaker: hs,
// 			Host:       host,
// 		}, sender).Run(ctx)
// 	}
//
// 	initClient := func(ctx context.Context, receiver protocol.MessageReceiver, hs handshake.Handshaker, dht dht.DHT) {
// 		defer GinkgoRecover()
// 		tcp.NewClient(
// 			tcp.ClientOptions{
// 				Logger:     logrus.StandardLogger(),
// 				Handshaker: hs,
// 			},
// 			tcp.NewConnPool(tcp.ConnPoolOptions{Timeout: 30 * time.Second, Handshaker: hs}), dht, receiver,
// 		).Run(ctx)
// 	}
//
// 	Context("when sending a valid message", func() {
// 		It("should successfully send and receive", func() {
// 			ctx, cancel := context.WithCancel(context.Background())
// 			// defer time.Sleep(time.Millisecond)
// 			defer cancel()
//
// 			fromServer := make(chan protocol.MessageOnTheWire, 1000)
// 			toClient := make(chan protocol.MessageOnTheWire, 1000)
//
// 			sender := testutil.NewSimpleTCPPeerAddress("sender", "127.0.0.1", "47325")
// 			receiver := testutil.NewSimpleTCPPeerAddress("receiver", "127.0.0.1", "47326")
// 			codec := testutil.NewSimpleTCPPeerAddressCodec()
// 			dht, err := dht.New(sender, codec, kv.NewTable(kv.NewMemDB(kv.JSONCodec), "dht"), receiver)
// 			Expect(err).Should(BeNil())
//
// 			// start TCP server and client
// 			go initServer(ctx, fromServer, nil, "127.0.0.1:47326")
// 			go initClient(ctx, toClient, nil, dht)
//
// 			check := func(x uint, y uint) bool {
// 				// Set variant and body size to an appropriate range
// 				v := int((x % 5) + 1)
// 				numBytes := int(y % 1000000) // 0 bytes to 1 megabyte
//
// 				// Generate random variant
// 				variant := protocol.MessageVariant(v)
// 				body := make([]byte, numBytes)
// 				if len(body) > 0 {
// 					n, err := rand.Read(body)
// 					Expect(n).To(Equal(numBytes))
// 					Expect(err).ToNot(HaveOccurred())
// 				}
//
// 				// Send a message to the server using the client and read from
// 				// message received by the server
// 				toClient <- protocol.MessageOnTheWire{
// 					To:      receiver.ID,
// 					Message: protocol.NewMessage(protocol.V1, variant, body),
// 				}
// 				messageOtw := <-fromServer
//
// 				// Check that the message sent equals the message received
// 				return (messageOtw.Message.Length == protocol.MessageLength(len(body)+8) &&
// 					reflect.DeepEqual(messageOtw.Message.Version, protocol.V1) &&
// 					reflect.DeepEqual(messageOtw.Message.Variant, variant) &&
// 					bytes.Compare(messageOtw.Message.Body, body) == 0)
// 			}
//
// 			Expect(quick.Check(check, &quick.Config{
// 				MaxCount: 100,
// 			})).Should(BeNil())
// 		})
//
// 		It("should successfully send and receive when client restarts after every message", func() {
// 			ctx, cancel := context.WithCancel(context.Background())
// 			// defer time.Sleep(time.Millisecond)
// 			defer cancel()
//
// 			fromServer := make(chan protocol.MessageOnTheWire, 1000)
// 			toClient := make(chan protocol.MessageOnTheWire, 1000)
//
// 			sender := testutil.NewSimpleTCPPeerAddress("sender", "127.0.0.1", "47325")
// 			receiver := testutil.NewSimpleTCPPeerAddress("receiver", "127.0.0.1", "47326")
// 			codec := testutil.NewSimpleTCPPeerAddressCodec()
// 			dht, err := dht.New(sender, codec, kv.NewTable(kv.NewMemDB(kv.JSONCodec), "dht"), receiver)
// 			Expect(err).Should(BeNil())
//
// 			// start TCP server and client
// 			go initServer(ctx, fromServer, nil, "127.0.0.1:47326")
//
// 			check := func(x uint, y uint) bool {
// 				childCTX, cancel := context.WithCancel(ctx)
// 				go initClient(childCTX, toClient, nil, dht)
// 				defer cancel()
//
// 				// Set variant and body size to an appropriate range
// 				v := int((x % 5) + 1)
// 				numBytes := int(y % 1000000) // 0 bytes to 1 megabyte
//
// 				// Generate random variant
// 				variant := protocol.MessageVariant(v)
// 				body := make([]byte, numBytes)
// 				if len(body) > 0 {
// 					n, err := rand.Read(body)
// 					Expect(n).To(Equal(numBytes))
// 					Expect(err).ToNot(HaveOccurred())
// 				}
//
// 				// Send a message to the server using the client and read from
// 				// message received by the server
// 				toClient <- protocol.MessageOnTheWire{
// 					To:      receiver.ID,
// 					Message: protocol.NewMessage(protocol.V1, variant, body),
// 				}
// 				messageOtw := <-fromServer
//
// 				// Check that the message sent equals the message received
// 				return (messageOtw.Message.Length == protocol.MessageLength(len(body)+8) &&
// 					reflect.DeepEqual(messageOtw.Message.Version, protocol.V1) &&
// 					reflect.DeepEqual(messageOtw.Message.Variant, variant) &&
// 					bytes.Compare(messageOtw.Message.Body, body) == 0)
// 			}
//
// 			Expect(quick.Check(check, &quick.Config{
// 				MaxCount: 100,
// 			})).Should(BeNil())
// 		})
//
// 		It("should successfully send and receive when both nodes are authenticated", func() {
// 			ctx, cancel := context.WithCancel(context.Background())
// 			// defer time.Sleep(time.Millisecond)
// 			defer cancel()
//
// 			fromServer := make(chan protocol.MessageOnTheWire, 1000)
// 			toClient := make(chan protocol.MessageOnTheWire, 1000)
//
// 			clientSignVerifier := testutil.NewMockSignVerifier()
// 			serverSignVerifier := testutil.NewMockSignVerifier(clientSignVerifier.ID())
// 			clientSignVerifier.Whitelist(serverSignVerifier.ID())
//
// 			sender := testutil.NewSimpleTCPPeerAddress(clientSignVerifier.ID(), "127.0.0.1", "47325")
// 			receiver := testutil.NewSimpleTCPPeerAddress(serverSignVerifier.ID(), "127.0.0.1", "47326")
// 			codec := testutil.NewSimpleTCPPeerAddressCodec()
// 			dht, err := dht.New(sender, codec, kv.NewTable(kv.NewMemDB(kv.JSONCodec), "dht"), receiver)
// 			Expect(err).Should(BeNil())
//
// 			// start TCP server and client
// 			go initServer(ctx, fromServer, handshake.New(serverSignVerifier, session.NewNOPSessionCreator()), "127.0.0.1:47326")
// 			go initClient(ctx, toClient, handshake.New(clientSignVerifier, session.NewNOPSessionCreator()), dht)
//
// 			check := func(x uint, y uint) bool {
// 				// Set variant and body size to an appropriate range
// 				v := int((x % 5) + 1)
// 				numBytes := int(y % 1000000) // 0 bytes to 1 megabyte
//
// 				// Generate random variant
// 				variant := protocol.MessageVariant(v)
// 				body := make([]byte, numBytes)
// 				if len(body) > 0 {
// 					n, err := rand.Read(body)
// 					Expect(n).To(Equal(numBytes))
// 					Expect(err).ToNot(HaveOccurred())
// 				}
//
// 				// Send a message to the server using the client and read from
// 				// message received by the server
// 				toClient <- protocol.MessageOnTheWire{
// 					To:      receiver.ID,
// 					Message: protocol.NewMessage(protocol.V1, variant, body),
// 				}
// 				messageOtw := <-fromServer
//
// 				// Check that the message sent equals the message received
// 				return (messageOtw.Message.Length == protocol.MessageLength(len(body)+8) &&
// 					reflect.DeepEqual(messageOtw.Message.Version, protocol.V1) &&
// 					reflect.DeepEqual(messageOtw.Message.Variant, variant) &&
// 					bytes.Compare(messageOtw.Message.Body, body) == 0)
// 			}
//
// 			Expect(quick.Check(check, &quick.Config{
// 				MaxCount: 100,
// 			})).Should(BeNil())
// 		})
//
// 		It("should successfully send and receive encrypted messages when both nodes are authenticated", func() {
// 			ctx, cancel := context.WithCancel(context.Background())
// 			// defer time.Sleep(time.Millisecond)
// 			defer cancel()
//
// 			fromServer := make(chan protocol.MessageOnTheWire, 1000)
// 			toClient := make(chan protocol.MessageOnTheWire, 1000)
//
// 			clientSignVerifier := testutil.NewMockSignVerifier()
// 			serverSignVerifier := testutil.NewMockSignVerifier(clientSignVerifier.ID())
// 			clientSignVerifier.Whitelist(serverSignVerifier.ID())
//
// 			sender := testutil.NewSimpleTCPPeerAddress(clientSignVerifier.ID(), "127.0.0.1", "47325")
// 			receiver := testutil.NewSimpleTCPPeerAddress(serverSignVerifier.ID(), "127.0.0.1", "47326")
// 			codec := testutil.NewSimpleTCPPeerAddressCodec()
// 			dht, err := dht.New(sender, codec, kv.NewTable(kv.NewMemDB(kv.JSONCodec), "dht"), receiver)
// 			Expect(err).Should(BeNil())
//
// 			// start TCP server and client
// 			go initServer(ctx, fromServer, handshake.New(serverSignVerifier, session.NewGCMSessionCreator()), "127.0.0.1:47326")
// 			go initClient(ctx, toClient, handshake.New(clientSignVerifier, session.NewGCMSessionCreator()), dht)
//
// 			check := func(x uint, y uint) bool {
// 				// Set variant and body size to an appropriate range
// 				v := int((x % 5) + 1)
// 				numBytes := int(y % 1000000) // 0 bytes to 1 megabyte
//
// 				// Generate random variant
// 				variant := protocol.MessageVariant(v)
// 				body := make([]byte, numBytes)
// 				if len(body) > 0 {
// 					n, err := rand.Read(body)
// 					Expect(n).To(Equal(numBytes))
// 					Expect(err).ToNot(HaveOccurred())
// 				}
//
// 				// Send a message to the server using the client and read from
// 				// message received by the server
// 				toClient <- protocol.MessageOnTheWire{
// 					To:      receiver.ID,
// 					Message: protocol.NewMessage(protocol.V1, variant, body),
// 				}
// 				messageOtw := <-fromServer
//
// 				// Check that the message sent equals the message received
// 				return (messageOtw.Message.Length == protocol.MessageLength(len(body)+8) &&
// 					reflect.DeepEqual(messageOtw.Message.Version, protocol.V1) &&
// 					reflect.DeepEqual(messageOtw.Message.Variant, variant) &&
// 					bytes.Compare(messageOtw.Message.Body, body) == 0)
// 			}
//
// 			Expect(quick.Check(check, &quick.Config{
// 				MaxCount: 100,
// 			})).Should(BeNil())
// 		})
//
// 		It("should successfully send and receive multicast messages", func() {
// 			ctx, cancel := context.WithCancel(context.Background())
// 			// defer time.Sleep(time.Millisecond)
// 			defer cancel()
//
// 			fromServerChans := make([]chan protocol.MessageOnTheWire, 8)
// 			receivers := make([]protocol.PeerAddress, 8)
// 			for i := range receivers {
// 				receivers[i] = testutil.NewSimpleTCPPeerAddress(fmt.Sprintf("receiver_%d", i), "127.0.0.1", fmt.Sprintf("%d", 47326+i))
// 				fromServerChans[i] = make(chan protocol.MessageOnTheWire, 100000)
// 				go initServer(ctx, fromServerChans[i], nil, fmt.Sprintf("127.0.0.1:%d", 47326+i))
// 			}
// 			codec := testutil.NewSimpleTCPPeerAddressCodec()
// 			toClientChans := make([]chan protocol.MessageOnTheWire, 8)
// 			for i := range toClientChans {
// 				client := testutil.NewSimpleTCPPeerAddress(fmt.Sprintf("sender_%d", i), "127.0.0.1", fmt.Sprintf("%d", 47325-i))
// 				dht, err := dht.New(client, codec, kv.NewTable(kv.NewMemDB(kv.JSONCodec), "dht"), receivers...)
// 				Expect(err).Should(BeNil())
// 				toClientChans[i] = make(chan protocol.MessageOnTheWire, 100000)
// 				go initClient(ctx, toClientChans[i], nil, dht)
// 			}
//
// 			check := func(x uint, y uint) bool {
// 				// Set variant and body size to an appropriate range
// 				// v := int((x % 5) + 1)
// 				numBytes := int(y % 1000000) // 0 bytes to 1 megabyte
//
// 				// Generate random variant
// 				// variant := protocol.MessageVariant(v)
// 				body := make([]byte, numBytes)
// 				if len(body) > 0 {
// 					n, err := rand.Read(body)
// 					Expect(n).To(Equal(numBytes))
// 					Expect(err).ToNot(HaveOccurred())
// 				}
//
// 				// Send a message to the server using the client and read from
// 				// message received by the server
// 				for _, toClient := range toClientChans {
// 					toClient <- protocol.MessageOnTheWire{
// 						Message: protocol.NewMessage(protocol.V1, protocol.Multicast, body),
// 					}
// 				}
// 				for range toClientChans {
// 					for _, fromServer := range fromServerChans {
// 						messageOtw := <-fromServer
// 						if !(messageOtw.Message.Length == protocol.MessageLength(len(body)+8) &&
// 							reflect.DeepEqual(messageOtw.Message.Version, protocol.V1) &&
// 							reflect.DeepEqual(messageOtw.Message.Variant, protocol.Multicast) &&
// 							bytes.Compare(messageOtw.Message.Body, body) == 0) {
// 							return false
// 						}
// 					}
// 				}
// 				// Check that the message sent equals the message received
// 				return true
// 			}
//
// 			Expect(quick.Check(check, &quick.Config{
// 				MaxCount: 100,
// 			})).Should(BeNil())
// 		})
// 	})
// })
