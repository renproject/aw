package tcp_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"go.uber.org/zap"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/tcp"
)

var _ = Describe("TCP", func() {
	capacity := 100

	Context("when connecting a client to a server", func() {
		Context("when the server is online", func() {
			It("should connect successfully", func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				receiver := make(chan wire.Message, capacity)
				serverOpts, clientOpts := defaultOpts()
				client, server := newClientAndServer(serverOpts, clientOpts, nil, receiver)

				go server.Listen(ctx)

				// Make sure that the server can accept connections. It is not
				// important for this to be certain, as the client will retry
				// if the connection is refused, but it would print an error
				// log.
				time.Sleep(10 * time.Millisecond)

				err := client.Send(ctx, serverAddr(server), wire.Message{Version: wire.V1, Type: wire.Ping})
				Expect(err).ToNot(HaveOccurred())
				Eventually(receiver).Should(Receive())
			})
		})

		Context("when the server is offline", func() {
			It("should retry", func() {
				Expect(true).To(BeTrue())
			})

			It("should eventually connect successfully when the server comes online", func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				receiver := make(chan wire.Message, capacity)
				serverOpts, clientOpts := defaultOpts()
				client, server := newClientAndServer(serverOpts, clientOpts, nil, receiver)

				err := client.Send(ctx, serverAddr(server), wire.Message{Version: wire.V1, Type: wire.Ping})
				Expect(err).ToNot(HaveOccurred())

				// Wait some time before turning on the server.
				time.Sleep(2 * client.Options().TimeToDial)
				go server.Listen(ctx)

				Eventually(receiver).Should(Receive())
			})
		})

		Context("when the client has too many connections", func() {
			Specify("sending should result in an error", func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				server1Opts, clientOpts := defaultOpts()
				clientOpts.MaxConnections = 1
				client, server1 := newClientAndServer(server1Opts, clientOpts, nil, nil)
				server2Opts, _ := defaultOpts()
				_, server2 := newClientAndServer(server2Opts, clientOpts, nil, nil)

				go server1.Listen(ctx)
				go server2.Listen(ctx)
				time.Sleep(10 * time.Millisecond)

				err := client.Send(ctx, serverAddr(server1), wire.Message{Version: wire.V1, Type: wire.Ping})
				Expect(err).ToNot(HaveOccurred())

				// Attempting to send to a new address should result in an
				// error.
				err = client.Send(ctx, serverAddr(server2), wire.Message{Version: wire.V1, Type: wire.Ping})
				Expect(err).To(HaveOccurred())

				// We should succeed if we close the previous connection.
				client.Close(serverAddr(server1))
				err = client.Send(ctx, serverAddr(server2), wire.Message{Version: wire.V1, Type: wire.Ping})
				Expect(err).ToNot(HaveOccurred())
			})
		})

		Context("when closing all connections in the client", func() {
			Specify("the client should be able to establish a full set of new connections", func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				_, clientOpts := defaultOpts()
				clientOpts.MaxConnections = 5
				client, _ := newClientAndServer(DefaultServerOptions(), clientOpts, nil, nil)

				addrs := make([]string, client.Options().MaxConnections)
				for i := 0; i < client.Options().MaxConnections; i++ {
					serverOpts, _ := defaultOpts()
					_, server := newClientAndServer(serverOpts, clientOpts, nil, nil)
					addrs[i] = serverAddr(server)
					go server.Listen(ctx)
				}
				time.Sleep(10 * time.Millisecond)

				// Have the client connect to all of the servers.
				for _, addr := range addrs {
					err := client.Send(ctx, addr, wire.Message{Version: wire.V1, Type: wire.Ping})
					Expect(err).ToNot(HaveOccurred())
				}

				// After closing all of the connctions, the client should be
				// able to connect to just as many new connections without
				// error.
				client.CloseAll()

				newAddrs := make([]string, client.Options().MaxConnections)
				for i := 0; i < client.Options().MaxConnections; i++ {
					serverOpts, _ := defaultOpts()
					_, server := newClientAndServer(serverOpts, clientOpts, nil, nil)
					newAddrs[i] = serverAddr(server)
					go server.Listen(ctx)
				}
				time.Sleep(10 * time.Millisecond)

				// Have the client connect to all of the servers.
				for _, addr := range newAddrs {
					err := client.Send(ctx, addr, wire.Message{Version: wire.V1, Type: wire.Ping})
					Expect(err).ToNot(HaveOccurred())
				}
			})
		})
	})
})

func BenchmarkSend(b *testing.B) {
	runBenchmarkSend(b, func(ctx context.Context, client *Client, server *Server) {
		addr := serverAddr(server)
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
	runBenchmarkSend(b, func(ctx context.Context, client *Client, server *Server) {
		addr := serverAddr(server)
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

func runBenchmarkSend(b *testing.B, run func(ctx context.Context, client *Client, server *Server)) {
	logger, err := defaultLogger()
	Expect(err).ToNot(HaveOccurred())
	serverPort := 3000 + (rand.Int() % 7000)
	serverOpts := DefaultServerOptions().
		WithLogger(logger).
		WithHost("127.0.0.1").
		WithPort(uint16(serverPort))
	clientOpts := DefaultClientOptions().
		WithLogger(logger).
		WithTimeToDial(10 * time.Millisecond).
		WithMaxCapacity(b.N)

	client, server := newClientAndServer(serverOpts, clientOpts, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go server.Listen(ctx)
	time.Sleep(time.Millisecond)

	defer func() {
		client.CloseAll()
		time.Sleep(time.Millisecond)
	}()

	// Dial initial connection.
	client.Send(ctx, serverAddr(server), wire.Message{Version: wire.V1, Type: wire.Ping})

	// Reset benchmark to isolate performance of sending messages.
	b.ReportAllocs()
	b.ResetTimer()
	run(ctx, client, server)
	b.StopTimer()
}

func defaultLogger() (*zap.Logger, error) {
	loggerConfig := zap.NewDevelopmentConfig()
	switch strings.ToLower(os.Getenv("LOG")) {
	case "panic":
		loggerConfig.Level.SetLevel(zap.PanicLevel)
	case "fatal":
		loggerConfig.Level.SetLevel(zap.FatalLevel)
	case "error":
		loggerConfig.Level.SetLevel(zap.ErrorLevel)
	case "warn":
		loggerConfig.Level.SetLevel(zap.WarnLevel)
	case "info":
		loggerConfig.Level.SetLevel(zap.InfoLevel)
	case "debug":
		loggerConfig.Level.SetLevel(zap.DebugLevel)
	default:
		loggerConfig.Level.SetLevel(zap.DebugLevel)
	}
	return loggerConfig.Build()
}

func defaultOpts() (ServerOptions, ClientOptions) {
	logger, err := defaultLogger()
	if err != nil {
		panic(err)
	}
	serverPort := 3000 + (rand.Int() % 7000)
	serverOpts := DefaultServerOptions().
		WithLogger(logger).
		WithHost("127.0.0.1").
		WithPort(uint16(serverPort))
	clientOpts := DefaultClientOptions().
		WithLogger(logger).
		WithTimeToDial(10 * time.Millisecond).
		WithMaxCapacity(100)
	return serverOpts, clientOpts
}

func newClientAndServer(serverOpts ServerOptions, clientOpts ClientOptions, clientRecv, serverRecv chan<- wire.Message) (*Client, *Server) {
	clientPrivKey := id.NewPrivKey()
	serverPrivKey := id.NewPrivKey()

	serverListener := wire.Callbacks{}
	if serverRecv != nil {
		serverListener.ReceivePing = func(version wire.Version, data []byte, from id.Signatory) (wire.Message, error) {
			serverRecv <- wire.Message{Version: version, Type: wire.Ping, Data: data}
			return wire.Message{Version: version, Type: wire.PingAck, Data: []byte{}}, nil
		}
	}

	server := NewServer(
		serverOpts,
		handshake.NewECDSA(handshake.DefaultOptions().WithPrivKey(serverPrivKey)),
		serverListener,
	)

	clientListener := wire.Callbacks{}
	client := NewClient(
		clientOpts,
		handshake.NewECDSA(handshake.DefaultOptions().WithPrivKey(clientPrivKey)),
		clientListener,
	)

	return client, server
}

func serverAddr(server *Server) string {
	return fmt.Sprintf("%v:%v", server.Options().Host, server.Options().Port)
}
