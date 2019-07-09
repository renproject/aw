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
	"github.com/sirupsen/logrus"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/tcp"
)

var _ = Describe("Tcp", func() {
	initServer := func(ctx context.Context, bind string, sender protocol.MessageSender) {
		logger := logrus.StandardLogger()
		err := NewServer(ServerOptions{
			Logger:  logger,
			Timeout: time.Minute,
		}, sender).Listen(ctx, bind)
		if err != nil {
			panic(err)
		}
	}

	initClient := func(ctx context.Context, receiver protocol.MessageReceiver) {
		logger := logrus.StandardLogger()
		NewClient(
			NewClientConns(ClientOptions{
				Logger:         logger,
				Timeout:        time.Minute,
				MaxConnections: 10,
			}),
			receiver,
		).Run(ctx)
	}

	Context("when sending a valid message", func() {
		It("should successfully send and receive", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer time.Sleep(100 * time.Millisecond)
			defer cancel()

			fromServer := make(chan protocol.MessageOnTheWire, 10)
			toClient := make(chan protocol.MessageOnTheWire, 10)

			// start TCP server and client
			go initServer(ctx, fmt.Sprintf("127.0.0.1:47326"), fromServer)
			go initClient(ctx, toClient)

			addrOfServer, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:47326"))
			Expect(err).Should(BeNil())

			iteration := func(v int, numBytes int) bool {
				// Set variant and body size to an appropriate range
				v = v%5 + 1
				numBytes = numBytes % 1000000 // 0 bytes to 1 megabyte

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
				messageWire := <-fromServer

				// Check that the message sent equals the message received
				return (messageWire.Message.Length == protocol.MessageLength(len(body)+32) &&
					reflect.DeepEqual(messageWire.Message.Version, protocol.V1) &&
					reflect.DeepEqual(messageWire.Message.Variant, variant) &&
					bytes.Compare(messageWire.Message.Body, body) == 0)
			}

			Expect(quick.Check(iteration, &quick.Config{
				MaxCount: 1000,
			})).Should(BeNil())
		})
	})
})
