package handshake_test

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/renproject/aw/codec"
	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/policy"
	"github.com/renproject/aw/tcp"
	"github.com/renproject/id"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Handshake", func() {
	Context("connecting a client to a server", func() {
		When("the server is online", func() {
			It("should connect successfully", func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var dialRetry, dialSuccess chan bool = nil, make(chan bool)
				portCh := listenOnAssignedPort(ctx)
				dial(ctx, <-portCh, dialRetry, dialSuccess)
				Expect(<-dialSuccess).Should(BeTrue())
			})
		})

		When("the server is offline", func() {
			It("should retry", func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var dialRetry, dialSuccess chan bool = make(chan bool), make(chan bool)
				dial(ctx, 3333, dialRetry, dialSuccess)
				Expect(<-dialRetry).Should(BeTrue())
			})

			It("if the server comes online should eventually connect successfully", func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var dialRetry, dialSuccess chan bool = make(chan bool), make(chan bool)
				port := 3334
				dial(ctx, port, dialRetry, dialSuccess)
				time.Sleep(500 * time.Millisecond)
				listen(ctx, port)
				Expect(<-dialRetry).Should(BeTrue())
				Expect(<-dialSuccess).Should(BeTrue())
			})
		})
	})
})

func listen(ctx context.Context, port int) {
	go func() {
		privKey := id.NewPrivKey()
		h := handshake.ECIES(privKey)

		tcp.Listen(ctx,
			fmt.Sprintf("127.0.0.1:%v", port),
			func(conn net.Conn) {
				h(conn,
					codec.PlainEncoder,
					codec.PlainDecoder,
				)
			},
			nil,
			nil,
		)
	}()
}

func listenOnAssignedPort(ctx context.Context) <-chan int {
	portCh := make(chan int, 1)
	go func() {
		privKey := id.NewPrivKey()
		h := handshake.ECIES(privKey)

		ip := "127.0.0.1"
		listener, port, err := tcp.ListenerWithAssignedPort(ctx, ip)
		Expect(err).ToNot(HaveOccurred())
		portCh <- port

		tcp.ListenWithListener(ctx,
			listener,
			func(conn net.Conn) {
				h(conn,
					codec.PlainEncoder,
					codec.PlainDecoder,
				)
			},
			nil,
			nil,
		)
	}()

	return portCh
}

func dial(ctx context.Context, port int, dialRetry, dialSuccess chan bool) {
	go func() {
		retrySignalOnce := sync.Once{}
		privKey := id.NewPrivKey()
		h := handshake.ECIES(privKey)

		tcp.Dial(ctx,
			fmt.Sprintf("127.0.0.1:%v", port),
			func(conn net.Conn) {
				_, _, _, err := h(conn,
					codec.PlainEncoder,
					codec.PlainDecoder)
				if err == nil {
					dialSuccess <- true
				}
			},
			func(error) {
				retrySignalOnce.Do(func() {
					dialRetry <- true
				})
			},
			policy.ConstantTimeout(50*time.Millisecond),
		)
	}()
}
