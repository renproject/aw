package tcp_test

import (
	"bytes"
	"context"
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
		err := NewServer(ServerOptions{
			Logger:  logrus.StandardLogger(),
			Timeout: time.Minute,
		}, sender).Listen(ctx, bind)
		if err != nil {
			panic(err)
		}
	}

	startClient := func(ctx context.Context, receiver protocol.MessageReceiver) {
		NewClient(
			NewClientConns(ClientOptions{
				Logger:         logrus.StandardLogger(),
				Timeout:        time.Minute,
				MaxConnections: 10,
			}),
			receiver,
		).Run(ctx)
	}

	Context("when sending a valid message", func() {
		It("should work", func() {
			sender := make(chan protocol.MessageOnTheWire, 10)
			receiver := make(chan protocol.MessageOnTheWire, 10)
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()

			go startServer(ctx, "0.0.0.0:49673", sender)
			go startClient(ctx, receiver)
			addr, err := net.ResolveTCPAddr("tcp", "0.0.0.0:49673")
			Expect(err).Should(BeNil())

			iteration := func(val uint16, body [16]byte) bool {
				variant := protocol.MessageVariant(val%5 + 1)
				receiver <- protocol.MessageOnTheWire{
					To:      addr,
					Message: protocol.NewMessage(protocol.V1, variant, body[:]),
				}
				msg := <-sender

				return (bytes.Compare(msg.Message.Body, body[:]) == 0 &&
					reflect.DeepEqual(msg.Message.Variant, variant) &&
					reflect.DeepEqual(msg.Message.Version, protocol.V1) &&
					msg.Message.Length == 48)
			}

			Expect(quick.Check(iteration, nil)).Should(BeNil())
		})
	})
})
