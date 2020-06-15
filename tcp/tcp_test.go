package tcp_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/tcp"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"github.com/sirupsen/logrus"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
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
	runBenchmarkSend(b, func(ctx context.Context, client *tcp.Client, server *tcp.Server) {
		addr := fmt.Sprintf("%v:%v", server.Options().Host, server.Options().Port)
		data := [32]byte{}
		for i := range data {
			data[i] = byte(rand.Int())
		}
		for i := 0; i < b.N; i++ {
			if err := client.Send(ctx, addr, wire.Message{Version: wire.V1, Type: wire.Push, Data: data[:]}); err != nil {
				b.Fatalf("sending: %v", err)
			}
		}
	})
}

func BenchmarkSendParallel(b *testing.B) {
	runBenchmarkSend(b, func(ctx context.Context, client *tcp.Client, server *tcp.Server) {
		addr := fmt.Sprintf("%v:%v", server.Options().Host, server.Options().Port)
		data := [32]byte{}
		for i := range data {
			data[i] = byte(rand.Int())
		}
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := client.Send(ctx, addr, wire.Message{Version: wire.V1, Type: wire.Push, Data: data[:]}); err != nil {
					b.Fatalf("sending: %v", err)
				}
			}
		})
	})
}

func runBenchmarkSend(b *testing.B, run func(ctx context.Context, client *tcp.Client, server *tcp.Server)) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := logrus.New()
	switch strings.ToLower(os.Getenv("LOG")) {
	case "panic":
		logger.SetLevel(logrus.PanicLevel)
	case "fatal":
		logger.SetLevel(logrus.FatalLevel)
	case "error":
		logger.SetLevel(logrus.ErrorLevel)
	case "warn":
		logger.SetLevel(logrus.WarnLevel)
	case "info":
		logger.SetLevel(logrus.InfoLevel)
	case "debug":
		logger.SetLevel(logrus.DebugLevel)
	case "trace":
		logger.SetLevel(logrus.TraceLevel)
	default:
		logger.SetOutput(ioutil.Discard)
	}

	clientPrivKey := id.NewPrivKey()
	serverPrivKey := id.NewPrivKey()

	serverPort := 3000 + (rand.Int() % 7000)
	serverListener := wire.Callbacks{}
	server := tcp.NewServer(
		tcp.DefaultServerOptions().
			WithLogger(logger).
			WithHost("127.0.0.1").
			WithPort(uint16(serverPort)),
		handshake.NewECDSA(handshake.DefaultOptions().WithPrivKey(serverPrivKey)),
		serverListener,
	)
	go server.Listen(ctx)
	time.Sleep(time.Millisecond)

	clientListener := wire.Callbacks{}
	client := tcp.NewClient(
		tcp.DefaultClientOptions().
			WithLogger(logger).
			WithMaxCapacity(b.N),
		handshake.NewECDSA(handshake.DefaultOptions().WithPrivKey(clientPrivKey)),
		clientListener,
	)
	defer func() {
		client.CloseAll()
		time.Sleep(time.Millisecond)
	}()

	// Dial initial connection.
	addr := fmt.Sprintf("127.0.0.1:%v", serverPort)
	client.Send(ctx, addr, wire.Message{Version: wire.V1, Type: wire.Ping})

	// Reset benchmark to isolate performance of sending messages.
	b.ReportAllocs()
	b.ResetTimer()
	run(ctx, client, server)
	b.StopTimer()
}
