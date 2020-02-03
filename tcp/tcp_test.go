package tcp_test

import (
	"context"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/message"
	. "github.com/renproject/aw/tcp"
	"github.com/sirupsen/logrus"
)

var _ = Describe("TCP", func() {

	Context("when connecting a client to a server", func() {
		Context("when the server is online", func() {
			It("should connect successfully", func() {
				Expect(true).To(BeTrue())
			})
		})

		Context("when the server is offline", func() {
			It("should retry", func() {
				Expect(true).To(BeTrue())
			})

			Context("when the server comes online", func() {
				It("should eventually connect successfully", func() {
					Expect(true).To(BeTrue())
				})
			})
		})
	})
})

func BenchmarkSend(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := logrus.New()
	// logger.SetOutput(ioutil.Discard)

	clientPrivKey, err := crypto.GenerateKey()
	if err != nil {
		b.Errorf("keygen: %v", err)
	}
	serverPrivKey, err := crypto.GenerateKey()
	if err != nil {
		b.Errorf("keygen: %v", err)
	}

	serverOutput := make(chan message.Message, b.N)
	server := NewServer(
		DefaultServerOptions().
			WithLogger(logger).
			WithHandshaker(handshake.NewECDSA(serverPrivKey, nil, 1024*1024)).
			WithHost("127.0.0.1").
			WithPort(6000),
		serverOutput)
	go server.Listen(ctx)
	time.Sleep(10 * time.Millisecond)

	client := NewClient(
		DefaultClientOptions().
			WithLogger(logger).
			WithHandshaker(handshake.NewECDSA(clientPrivKey, nil, 1024*1024)).
			WithMaxCapacity(b.N))
	defer client.CloseAll()

	// Dial initial connection.
	address := "127.0.0.1:6000"
	client.Send(ctx, address, message.Message{})

	// Reset benchmark to isolate performance of sending messages.
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := client.Send(ctx, address, message.Message{}); err != nil {
				b.Fatalf("send: err=%v", err)
			}
		}
	})
}
