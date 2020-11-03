package aw_test

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync/atomic"
	"time"

	"github.com/renproject/experiment/channel"
	"github.com/renproject/experiment/codec"
	"github.com/renproject/experiment/handshake"
	"github.com/renproject/experiment/policy"
	"github.com/renproject/experiment/tcp"
	"github.com/renproject/id"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("TCP", func() {
	Describe("Channel", func() {
		Context("when a pair of nodes are trying to establish connections both ways", func() {
			It("should only maintain one connection", func() {
				pool1 := channel.NewPool(channel.DefaultPoolOptions())
				pool2 := channel.NewPool(channel.DefaultPoolOptions())

				privKey1 := id.NewPrivKey()
				privKey2 := id.NewPrivKey()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				var connectionKillCount int64 = 0
				go func() {
					tcp.Listen(ctx,
						"localhost:12345",
						func(conn net.Conn) {
							h := handshake.ECIESServerHandshake(privKey1, rand.New(rand.NewSource(time.Now().UnixNano())))
							h, _ = pool1.HighestPeerWinsHandshake(privKey1.Signatory(), h)
							_, _, _, err := h(conn, codec.LengthPrefixEncoder(codec.PlainEncoder), codec.LengthPrefixDecoder(codec.PlainDecoder))
							if err != nil {
								fmt.Printf("%v - server side \n", err)
								atomic.AddInt64(&connectionKillCount, 1)
							}
							<-time.After(time.Second * 1)
						},
						nil,
						policy.Max(2),
					)
				}()

				go func() {
					tcp.Listen(ctx,
						"localhost:12346",
						func(conn net.Conn) {
							h := handshake.ECIESServerHandshake(privKey2, rand.New(rand.NewSource(time.Now().UnixNano())))
							h, _ = pool2.HighestPeerWinsHandshake(privKey2.Signatory(), h)
							_, _, _, err := h(conn, codec.LengthPrefixEncoder(codec.PlainEncoder), codec.LengthPrefixDecoder(codec.PlainDecoder))
							if err != nil {
								fmt.Printf("%v - server side \n", err)
								atomic.AddInt64(&connectionKillCount, 1)
							}
							<-time.After(time.Second * 1)
						},
						nil,
						policy.Max(2),
					)
				}()

				go func() {
					tcp.Dial(ctx,
						"localhost:12346",
						func(conn net.Conn) {
							h := handshake.ECIESClientHandshake(privKey1, rand.New(rand.NewSource(time.Now().UnixNano())))
							h, _ = pool1.HighestPeerWinsHandshake(privKey1.Signatory(), h)
							_, _, _, err := h(conn, codec.LengthPrefixEncoder(codec.PlainEncoder), codec.LengthPrefixDecoder(codec.PlainDecoder))
							if err != nil {
								fmt.Printf("%v - client side 1\n", err)
								atomic.AddInt64(&connectionKillCount, 1)
							}
						},
						nil,
						policy.ConstantTimeout(time.Second*2),
					)
				}()

				tcp.Dial(ctx,
					"localhost:12345",
					func(conn net.Conn) {
						h := handshake.ECIESClientHandshake(privKey2, rand.New(rand.NewSource(time.Now().UnixNano())))
						h, _ = pool2.HighestPeerWinsHandshake(privKey2.Signatory(), h)
						_, _, _, err := h(conn, codec.LengthPrefixEncoder(codec.PlainEncoder), codec.LengthPrefixDecoder(codec.PlainDecoder))
						if err != nil {
							fmt.Printf("%v - client side 2\n", err)
							atomic.AddInt64(&connectionKillCount, 1)
						}
					},
					nil,
					policy.ConstantTimeout(time.Second*2),
				)

				<-time.After(time.Second * 1)
				Expect(connectionKillCount).To(Equal(int64(2)))

			})
		})

		Context("when a client tries to establish two connections to a server", func() {
			It("should only maintain one connection", func() {
				pool1 := channel.NewPool(channel.DefaultPoolOptions())
				pool2 := channel.NewPool(channel.DefaultPoolOptions())

				privKey1 := id.NewPrivKey()
				privKey2 := id.NewPrivKey()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				var connectionKillCount int64 = 0
				go func() {
					tcp.Listen(ctx,
						"localhost:12345",
						func(conn net.Conn) {
							h := handshake.ECIESServerHandshake(privKey1, rand.New(rand.NewSource(time.Now().UnixNano())))
							h, _ = pool1.HighestPeerWinsHandshake(privKey1.Signatory(), h)
							_, _, _, err := h(conn, codec.LengthPrefixEncoder(codec.PlainEncoder), codec.LengthPrefixDecoder(codec.PlainDecoder))
							if err != nil {
								fmt.Printf("%v - server side \n", err)
								atomic.AddInt64(&connectionKillCount, 1)
							}
							<-time.After(time.Second * 1)
						},
						nil,
						policy.Max(2),
					)
				}()

				go func() {
					tcp.Dial(ctx,
						"localhost:12345",
						func(conn net.Conn) {
							h := handshake.ECIESClientHandshake(privKey2, rand.New(rand.NewSource(time.Now().UnixNano())))
							h, _ = pool2.HighestPeerWinsHandshake(privKey2.Signatory(), h)
							_, _, _, err := h(conn, codec.LengthPrefixEncoder(codec.PlainEncoder), codec.LengthPrefixDecoder(codec.PlainDecoder))
							if err != nil {
								fmt.Printf("%v - client side \n", err)
								atomic.AddInt64(&connectionKillCount, 1)
							}
						},
						nil,
						policy.ConstantTimeout(time.Second*2),
					)
				}()

				tcp.Dial(ctx,
					"localhost:12345",
					func(conn net.Conn) {
						h := handshake.ECIESClientHandshake(privKey2, rand.New(rand.NewSource(time.Now().UnixNano())))
						h, _ = pool2.HighestPeerWinsHandshake(privKey2.Signatory(), h)
						_, _, _, err := h(conn, codec.LengthPrefixEncoder(codec.PlainEncoder), codec.LengthPrefixDecoder(codec.PlainDecoder))
						if err != nil {
							fmt.Printf("%v - client side \n", err)
							atomic.AddInt64(&connectionKillCount, 1)
						}
					},
					nil,
					policy.ConstantTimeout(time.Second*2),
				)

				<-time.After(time.Second * 1)
				Expect(connectionKillCount).To(Equal(int64(2)))

			})
		})
	})
})
