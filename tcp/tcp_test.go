package tcp_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"reflect"
	"testing/quick"
	"time"

	"github.com/renproject/aw/protocol"
	"github.com/renproject/aw/tcp"
	"github.com/renproject/aw/testutil"
	"github.com/sirupsen/logrus"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Tcp", func() {
	initServer := func(ctx context.Context, bind string, sender protocol.MessageSender, sv protocol.SignVerifier) {
		err := tcp.NewServer(tcp.ServerOptions{
			SignVerifier: sv,
			Logger:       logrus.StandardLogger(),
			Timeout:      time.Minute,
		}, sender).Listen(ctx, bind)
		if err != nil {
			panic(err)
		}
	}

	initClient := func(ctx context.Context, receiver protocol.MessageReceiver, sv protocol.SignVerifier) {
		tcp.NewClient(
			tcp.NewClientConns(tcp.ClientOptions{
				SignVerifier:   sv,
				Logger:         logrus.StandardLogger(),
				Timeout:        time.Minute,
				MaxConnections: 10,
			}),
			receiver,
		).Run(ctx)
	}

	Context("when sending a valid message", func() {
		It("should successfully send and receive when both nodes are authenticated", func() {
			ctx, cancel := context.WithCancel(context.Background())
			// defer time.Sleep(time.Millisecond)
			defer cancel()

			fromServer := make(chan protocol.MessageOnTheWire, 1000)
			toClient := make(chan protocol.MessageOnTheWire, 1000)

			clientSignVerifier := testutil.NewMockSignVerifier()
			serverSignVerifier := testutil.NewMockSignVerifier(clientSignVerifier.ID())
			clientSignVerifier.Whitelist(serverSignVerifier.ID())

			// start TCP server and client
			go initServer(ctx, fmt.Sprintf("127.0.0.1:47326"), fromServer, serverSignVerifier)
			go initClient(ctx, toClient, clientSignVerifier)

			addrOfServer, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:47326"))
			Expect(err).Should(BeNil())

			check := func(x uint, y uint) bool {
				// Set variant and body size to an appropriate range
				v := int((x % 5) + 1)
				numBytes := int(y % 1000000) // 0 bytes to 1 megabyte

				// Generate random variant
				variant := protocol.MessageVariant(v)
				body := make([]byte, numBytes)
				if len(body) > 0 {
					n, err := rand.Read(body)
					Expect(n).To(Equal(numBytes))
					Expect(err).ToNot(HaveOccurred())
				}

				// Send a message to the server using the client and read from
				// message received by the server
				toClient <- protocol.MessageOnTheWire{
					To:      addrOfServer,
					Message: protocol.NewMessage(protocol.V1, variant, body),
				}
				messageOtw := <-fromServer

				// Check that the message sent equals the message received
				return (messageOtw.Message.Length == protocol.MessageLength(len(body)+32) &&
					reflect.DeepEqual(messageOtw.Message.Version, protocol.V1) &&
					reflect.DeepEqual(messageOtw.Message.Variant, variant) &&
					bytes.Compare(messageOtw.Message.Body, body) == 0)
			}

			Expect(quick.Check(check, &quick.Config{
				MaxCount: 100,
			})).Should(BeNil())
		})
	})
})
