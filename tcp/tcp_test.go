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

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/tcp"

	"github.com/renproject/aw/protocol"
	"github.com/sirupsen/logrus"
)

var _ = Describe("Tcp", func() {
	startServer := func(ctx context.Context, bind string, sender protocol.MessageSender) {
		logger := logrus.StandardLogger()
		err := NewServer(ServerOptions{
			Logger:  logger,
			Timeout: time.Minute,
		}, sender).Listen(ctx, bind)
		if err != nil {
			panic(err)
		}
	}

	startClient := func(ctx context.Context, receiver protocol.MessageReceiver) {
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
			sender := make(chan protocol.MessageOnTheWire, 10)
			receiver := make(chan protocol.MessageOnTheWire, 10)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// start TCP server and client
			go startServer(ctx, fmt.Sprintf("127.0.0.1:47326"), sender)
			go startClient(ctx, receiver)

			addr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("127.0.0.1:47326"))
			Expect(err).Should(BeNil())

			iteration := func(val uint32) bool {
				// create message body of random length between 2 Bytes and
				// 4 Mega Bytes
				body := make([]byte, int(val/1000)+2)
				rand.Read(body)

				variant := protocol.MessageVariant(val%5 + 1)
				receiver <- protocol.MessageOnTheWire{
					To:      addr,
					Message: protocol.NewMessage(protocol.V1, variant, body),
				}
				msg := <-sender

				return (bytes.Compare(msg.Message.Body, body) == 0 &&
					reflect.DeepEqual(msg.Message.Variant, variant) &&
					reflect.DeepEqual(msg.Message.Version, protocol.V1) &&
					msg.Message.Length == protocol.MessageLength(len(body)+32))
			}

			Expect(quick.Check(iteration, &quick.Config{
				MaxCount: 10,
			})).Should(BeNil())
		})
	})
})
