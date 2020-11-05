package handshake_test

import (
	"context"
	"math/rand"
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

	run := func(ctx context.Context, dialAfter, listenAfter time.Duration, shouldListen bool, dialRetry, dialSuccess chan bool) {
		if shouldListen {
			go func() {
				time.Sleep(0)
				<-time.After(listenAfter)

				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				privKey := id.NewPrivKey()
				h := handshake.ECIES(privKey, r)

				tcp.Listen(ctx,
					"127.0.0.1:3334",
					func(conn net.Conn) {
						h(conn,
							codec.LengthPrefixEncoder(codec.PlainEncoder),
							codec.LengthPrefixDecoder(codec.PlainDecoder),
						)
					},
					nil,
					nil,
				)
			}()
		}

		go func() {
			time.Sleep(0)
			<-time.After(dialAfter)

			retrySignalOnce := sync.Once{}
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			privKey := id.NewPrivKey()
			h := handshake.ECIES(privKey, r)

			tcp.Dial(ctx,
				"127.0.0.1:3334",
				func(conn net.Conn) {
					_, _, _, err := h(conn,
						codec.LengthPrefixEncoder(codec.PlainEncoder),
						codec.LengthPrefixDecoder(codec.PlainDecoder))
					if err == nil {
						dialSuccess <- true
					}
				},
				func() func(error) {
					return func(error) {
						retrySignalOnce.Do(func() {
							dialRetry <- true
						})
					}
				}(),
				policy.ConstantTimeout(50*time.Millisecond),
			)
		}()
	}

	Context("when connecting a client to a server", func() {
		Context("when the server is online", func() {
			It("should connect successfully", func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var dialRetry, dialSuccess chan bool = nil, make(chan bool)
				run(ctx, 500*time.Millisecond, 0, true, dialRetry, dialSuccess)
				Expect(<-dialSuccess).Should(BeTrue())
			})
		})

		Context("when the server is offline", func() {
			It("should retry", func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var dialRetry, dialSuccess chan bool = make(chan bool), make(chan bool)
				run(ctx, 0, 500*time.Millisecond, false, dialRetry, dialSuccess)
				Expect(<-dialRetry).Should(BeTrue())
			})

			Context("when the server comes online", func() {
				It("should eventually connect successfully", func() {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					var dialRetry, dialSuccess chan bool = make(chan bool), make(chan bool)
					run(ctx, 0, 500*time.Millisecond, true, dialRetry, dialSuccess)
					Expect(<-dialRetry).Should(BeTrue())
					Expect(<-dialSuccess).Should(BeTrue())
				})
			})
		})
	})
})
