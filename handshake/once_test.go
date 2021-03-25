package handshake_test

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
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
	Describe("Once", func() {
		Context("when a pair of nodes are trying to establish connections both ways", func() {
			It("should only maintain one connection", func() {
				pool1 := handshake.NewOncePool(handshake.DefaultOncePoolOptions())
				pool2 := handshake.NewOncePool(handshake.DefaultOncePoolOptions())

				privKey1 := id.NewPrivKey()
				privKey2 := id.NewPrivKey()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				handshakeDone1 := make(chan struct{}, 1)
				handshakeDone2 := make(chan struct{}, 1)
				serverHandshakeDone := make(chan struct{}, 2)

				var connectionKillCount int64 = 0
				go func() {
					tcp.Listen(ctx,
						"localhost:12345",
						func(conn net.Conn) {
							h := handshake.ECIES(privKey1)
							h = handshake.Once(privKey1.Signatory(), &pool1, h)
							_, _, _, err := h(conn, codec.PlainEncoder, codec.PlainDecoder)
							if err != nil {
								fmt.Printf("%v - server side \n", err)
								atomic.AddInt64(&connectionKillCount, 1)
							}
							serverHandshakeDone <- struct{}{}
						},
						nil,
						policy.Max(2),
					)
				}()

				go func() {
					tcp.Listen(ctx,
						"localhost:12346",
						func(conn net.Conn) {
							h := handshake.ECIES(privKey2)
							h = handshake.Once(privKey2.Signatory(), &pool2, h)
							_, _, _, err := h(conn, codec.PlainEncoder, codec.PlainDecoder)
							if err != nil {
								fmt.Printf("%v - server side \n", err)
								atomic.AddInt64(&connectionKillCount, 1)
							}
							serverHandshakeDone <- struct{}{}
						},
						nil,
						policy.Max(2),
					)
				}()
				<-time.After(100 * time.Millisecond)

				go func() {
					tcp.Dial(ctx,
						"localhost:12346",
						func(conn net.Conn) {
							h := handshake.ECIES(privKey1)
							h = handshake.Once(privKey1.Signatory(), &pool1, h)
							_, _, _, err := h(conn, codec.PlainEncoder, codec.PlainDecoder)
							if err != nil {
								fmt.Printf("%v - client side 1\n", err)
								atomic.AddInt64(&connectionKillCount, 1)
							}
							handshakeDone1 <- struct{}{}
						},
						nil,
						policy.ConstantTimeout(time.Second*2),
					)
				}()

				tcp.Dial(ctx,
					"localhost:12345",
					func(conn net.Conn) {
						h := handshake.ECIES(privKey2)
						h = handshake.Once(privKey2.Signatory(), &pool2, h)
						_, _, _, err := h(conn, codec.PlainEncoder, codec.PlainDecoder)
						if err != nil {
							fmt.Printf("%v - client side 2\n", err)
							atomic.AddInt64(&connectionKillCount, 1)
						}
						handshakeDone2 <- struct{}{}
					},
					nil,
					policy.ConstantTimeout(time.Second*2),
				)

				<-handshakeDone1
				<-handshakeDone2
				<-serverHandshakeDone
				<-serverHandshakeDone

				Expect(atomic.LoadInt64(&connectionKillCount)).To(Equal(int64(2)))

			})
		})

		Context("when a client tries to establish two connections to a server", func() {
			It("should only maintain one connection", func() {
				pool1 := handshake.NewOncePool(handshake.DefaultOncePoolOptions())
				pool2 := handshake.NewOncePool(handshake.DefaultOncePoolOptions())

				privKey1 := id.NewPrivKey()
				privKey2 := id.NewPrivKey()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				handshakeDone1 := make(chan struct{}, 1)
				handshakeDone2 := make(chan struct{}, 1)
				serverHandshakeDone := make(chan struct{}, 2)

				var connectionKillCount int64 = 0
				go func() {
					tcp.Listen(ctx,
						"localhost:12345",
						func(conn net.Conn) {
							h := handshake.ECIES(privKey1)
							h = handshake.Once(privKey1.Signatory(), &pool1, h)
							_, _, _, err := h(conn, codec.PlainEncoder, codec.PlainDecoder)
							if err != nil {
								fmt.Printf("%v - server side \n", err)
								atomic.AddInt64(&connectionKillCount, 1)
							}
							serverHandshakeDone <- struct{}{}
						},
						nil,
						policy.Max(2),
					)
				}()
				<-time.After(100 * time.Millisecond)

				go func() {
					tcp.Dial(ctx,
						"localhost:12345",
						func(conn net.Conn) {
							h := handshake.ECIES(privKey2)
							h = handshake.Once(privKey2.Signatory(), &pool2, h)
							_, _, _, err := h(conn, codec.PlainEncoder, codec.PlainDecoder)
							if err != nil {
								fmt.Printf("%v - client side \n", err)
								atomic.AddInt64(&connectionKillCount, 1)
							}
							handshakeDone1 <- struct{}{}
						},
						nil,
						policy.ConstantTimeout(time.Second*2),
					)
				}()

				tcp.Dial(ctx,
					"localhost:12345",
					func(conn net.Conn) {
						h := handshake.ECIES(privKey2)
						h = handshake.Once(privKey2.Signatory(), &pool2, h)
						_, _, _, err := h(conn, codec.PlainEncoder, codec.PlainDecoder)
						if err != nil {
							fmt.Printf("%v - client side \n", err)
							atomic.AddInt64(&connectionKillCount, 1)
						}
						handshakeDone2 <- struct{}{}
					},
					nil,
					policy.ConstantTimeout(time.Second*2),
				)

				<-handshakeDone1
				<-handshakeDone2
				<-serverHandshakeDone
				<-serverHandshakeDone

				Expect(atomic.LoadInt64(&connectionKillCount)).To(Equal(int64(2)))
			})
		})
	})
})
