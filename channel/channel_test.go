package channel_test

import (
	"context"
	"encoding/binary"
	"log"
	"net"
	"time"

	"github.com/renproject/experiment/channel"
	"github.com/renproject/experiment/codec"
	"github.com/renproject/experiment/handshake"
	"github.com/renproject/experiment/policy"
	"github.com/renproject/experiment/tcp"
	"github.com/renproject/experiment/wire"
	"github.com/renproject/id"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Channels", func() {
	Context("when sending and receiving messages", func() {
		/*		Context("when a connection is attached before", func() {
					It("should send and receive all messages", func() {
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()

						localPrivKey := id.NewPrivKey()
						remotePrivKey := id.NewPrivKey()

						localInbound, localOutbound := make(chan wire.Msg), make(chan wire.Msg)
						localCh := channel.New(remotePrivKey.Signatory(), localInbound, localOutbound)
						go func() {
							defer GinkgoRecover()
							if err := localCh.Run(ctx); err != nil {
								log.Printf("run: %v", err)
								return
							}
						}()
						go func() {
							defer GinkgoRecover()
							Expect(tcp.Listen(
								ctx,
								"127.0.0.1:3335",
								func(conn net.Conn) {
									log.Printf("accepted: %v", conn.RemoteAddr())
									local := localPrivKey.Signatory()
									enc, dec, remote, err := handshake.Insecure(local)(
										conn,
										codec.LengthPrefixEncoder(codec.PlainEncoder),
										codec.LengthPrefixDecoder(codec.PlainDecoder),
									)
									if err != nil {
										log.Printf("handshake: %v", err)
										return
									}
									if !remotePrivKey.Signatory().Equal(&remote) {
										log.Printf("handshake: expected %v, got %v", remotePrivKey.Signatory(), remote)
										return
									}
									if err := localCh.Attach(ctx, conn, enc, dec); err != nil {
										log.Printf("attach: %v", err)
										return
									}
								},
								func(err error) {
									log.Printf("listen: %v", err)
								},
								nil,
							)).To(Succeed())
						}()

						remoteInbound, remoteOutbound := make(chan wire.Msg), make(chan wire.Msg)
						remoteCh := channel.New(localPrivKey.Signatory(), remoteInbound, remoteOutbound)

						go func() {
							defer GinkgoRecover()
							if err := remoteCh.Run(ctx); err != nil {
								log.Printf("run: %v", err)
								return
							}
						}()
						go func() {
							defer GinkgoRecover()
							Expect(tcp.Dial(
								ctx,
								"127.0.0.1:3335",
								func(conn net.Conn) {
									log.Printf("dialed: %v", conn.RemoteAddr())
									remote := remotePrivKey.Signatory()
									enc, dec, local, err := handshake.Insecure(remote)(
										conn,
										codec.LengthPrefixEncoder(codec.PlainEncoder),
										codec.LengthPrefixDecoder(codec.PlainDecoder),
									)
									if err != nil {
										log.Printf("handshake: %v", err)
										return
									}
									if !localPrivKey.Signatory().Equal(&local) {
										log.Printf("handshake: expected %v, got %v", localPrivKey.Signatory(), local)
										return
									}
									if err := remoteCh.Attach(ctx, conn, enc, dec); err != nil {
										log.Printf("attach: %v", err)
										return
									}
								},
								func(err error) {
									log.Printf("dial: %v", err)
								},
								nil,
							)).To(Succeed())
						}()

						// Write messages from local to remote.
						d1, d2 := make(chan struct{}), make(chan struct{})
						n := uint64(1000)
						go func() {
							for iter := uint64(0); iter < n; iter++ {
								data := [8]byte{}
								binary.BigEndian.PutUint64(data[:], iter)
								localOutbound <- wire.Msg{Data: data[:]}
							}
						}()
						// Read messages from local to remote.
						go func() {
							defer GinkgoRecover()
							defer close(d1)
							for iter := uint64(0); iter < n; iter++ {
								msg := <-remoteInbound
								data := binary.BigEndian.Uint64(msg.Data)
								Expect(data).To(Equal(iter))
							}
						}()
						// Write messages from remote to local.
						go func() {
							for iter := uint64(0); iter < n; iter++ {
								data := [8]byte{}
								binary.BigEndian.PutUint64(data[:], iter)
								remoteOutbound <- wire.Msg{Data: data[:]}
							}
						}()
						// Read messages from remote to local.
						go func() {
							defer GinkgoRecover()
							defer close(d2)
							for iter := uint64(0); iter < n; iter++ {
								msg := <-localInbound
								data := binary.BigEndian.Uint64(msg.Data)
								Expect(data).To(Equal(iter))
							}
						}()
						<-d1
						<-d2
					})
				})

				Context("when a connection is attached after", func() {
					It("should send and receive all messages", func() {
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()

						localPrivKey := id.NewPrivKey()
						remotePrivKey := id.NewPrivKey()

						// Run the local channel.
						localInbound, localOutbound := make(chan wire.Msg), make(chan wire.Msg)
						localCh := channel.New(remotePrivKey.Signatory(), localInbound, localOutbound)
						go func() {
							defer GinkgoRecover()
							if err := localCh.Run(ctx); err != nil {
								log.Printf("run: %v", err)
								return
							}
						}()

						// Run the remote channel.
						remoteInbound, remoteOutbound := make(chan wire.Msg), make(chan wire.Msg)
						remoteCh := channel.New(localPrivKey.Signatory(), remoteInbound, remoteOutbound)
						go func() {
							defer GinkgoRecover()
							if err := remoteCh.Run(ctx); err != nil {
								log.Printf("run: %v", err)
								return
							}
						}()

						// Write messages from local to remote.
						d1, d2 := make(chan struct{}), make(chan struct{})
						n := uint64(1000)
						go func() {
							for iter := uint64(0); iter < n; iter++ {
								data := [8]byte{}
								binary.BigEndian.PutUint64(data[:], iter)
								localOutbound <- wire.Msg{Data: data[:]}
							}
						}()
						// Read messages from local to remote.
						go func() {
							defer GinkgoRecover()
							defer close(d1)
							for iter := uint64(0); iter < n; iter++ {
								msg := <-remoteInbound
								data := binary.BigEndian.Uint64(msg.Data)
								Expect(data).To(Equal(iter))
							}
						}()
						// Write messages from remote to local.
						go func() {
							for iter := uint64(0); iter < n; iter++ {
								data := [8]byte{}
								binary.BigEndian.PutUint64(data[:], iter)
								remoteOutbound <- wire.Msg{Data: data[:]}
							}
						}()
						// Read messages from remote to local.
						go func() {
							defer GinkgoRecover()
							defer close(d2)
							for iter := uint64(0); iter < n; iter++ {
								msg := <-localInbound
								data := binary.BigEndian.Uint64(msg.Data)
								Expect(data).To(Equal(iter))
							}
						}()

						// Wait for some backlog to build up.
						time.Sleep(100 * time.Millisecond)

						// Listen for incoming connections.
						go func() {
							defer GinkgoRecover()
							Expect(tcp.Listen(
								ctx,
								"127.0.0.1:3335",
								func(conn net.Conn) {
									log.Printf("accepted: %v", conn.RemoteAddr())
									local := localPrivKey.Signatory()
									enc, dec, remote, err := handshake.Insecure(local)(
										conn,
										codec.LengthPrefixEncoder(codec.PlainEncoder),
										codec.LengthPrefixDecoder(codec.PlainDecoder),
									)
									if err != nil {
										log.Printf("handshake: %v", err)
										return
									}
									if !remotePrivKey.Signatory().Equal(&remote) {
										log.Printf("handshake: expected %v, got %v", remotePrivKey.Signatory(), remote)
										return
									}
									if err := localCh.Attach(ctx, conn, enc, dec); err != nil {
										log.Printf("attach: %v", err)
										return
									}
								},
								func(err error) {
									log.Printf("listen: %v", err)
								},
								nil,
							)).To(Equal(context.Canceled))
						}()
						// Dial a connection.
						go func() {
							defer GinkgoRecover()
							Expect(tcp.Dial(
								ctx,
								"127.0.0.1:3335",
								func(conn net.Conn) {
									log.Printf("dialed: %v", conn.RemoteAddr())
									remote := remotePrivKey.Signatory()
									enc, dec, local, err := handshake.Insecure(remote)(
										conn,
										codec.LengthPrefixEncoder(codec.PlainEncoder),
										codec.LengthPrefixDecoder(codec.PlainDecoder),
									)
									if err != nil {
										log.Printf("handshake: %v", err)
										return
									}
									if !localPrivKey.Signatory().Equal(&local) {
										log.Printf("handshake: expected %v, got %v", localPrivKey.Signatory(), local)
										return
									}
									if err := remoteCh.Attach(ctx, conn, enc, dec); err != nil {
										log.Printf("attach: %v", err)
										return
									}
								},
								func(err error) {
									log.Printf("dial: %v", err)
								},
								nil,
							)).To(Succeed())
						}()

						// Wait for the test to be done.
						<-d1
						<-d2
					})
				})
		*/
		Context("when a connection is replaced", func() {
			It("should send and receive all messages", func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				localPrivKey := id.NewPrivKey()
				remotePrivKey := id.NewPrivKey()

				// Run the local channel.
				localInbound, localOutbound := make(chan wire.Msg), make(chan wire.Msg)
				localCh := channel.New(remotePrivKey.Signatory(), localInbound, localOutbound)
				go func() {
					defer GinkgoRecover()
					if err := localCh.Run(ctx); err != nil {
						log.Printf("run: %v", err)
						return
					}
				}()

				// Run the remote channel.
				remoteInbound, remoteOutbound := make(chan wire.Msg), make(chan wire.Msg)
				remoteCh := channel.New(localPrivKey.Signatory(), remoteInbound, remoteOutbound)
				go func() {
					defer GinkgoRecover()
					if err := remoteCh.Run(ctx); err != nil {
						log.Printf("run: %v", err)
						return
					}
				}()

				// Write messages from local to remote.
				d1 := make(chan struct{})
				n := uint64(500000)
				go func() {
					for iter := uint64(0); iter < n; iter++ {
						data := [8]byte{}
						binary.BigEndian.PutUint64(data[:], iter)
						localOutbound <- wire.Msg{Data: data[:]}
						log.Printf("local write: %v", iter)
					}
					log.Printf("local write: done")
				}()
				// Read messages from local to remote.
				go func() {
					defer GinkgoRecover()
					defer close(d1)
					for iter := uint64(0); iter < n; iter++ {
						msg := <-remoteInbound
						data := binary.BigEndian.Uint64(msg.Data)
						// Expect(data).To(Equal(iter))
						log.Printf("remote read: %v", data)
						if data >= n-1 {
							break
						}
					}
					log.Printf("remote read: done")
				}()

				// Listen for incoming connections.
				go func() {
					defer GinkgoRecover()

					Expect(tcp.Listen(
						ctx,
						"127.0.0.1:3335",
						func(conn net.Conn) {
							log.Printf("accepted: %v", conn.RemoteAddr())
							local := localPrivKey.Signatory()
							enc, dec, remote, err := handshake.Insecure(local)(
								conn,
								codec.LengthPrefixEncoder(codec.PlainEncoder),
								codec.LengthPrefixDecoder(codec.PlainDecoder),
							)
							if err != nil {
								log.Printf("handshake: %v", err)
								return
							}
							if !remotePrivKey.Signatory().Equal(&remote) {
								log.Printf("handshake: expected %v, got %v", remotePrivKey.Signatory(), remote)
								return
							}
							if err := localCh.Attach(ctx, conn, enc, dec); err != nil {
								// log.Printf("attach: %v", err)
								return
							}
						},
						func(err error) {
							// log.Printf("listen: %v", err)
						},
						nil,
					)).To(Equal(context.Canceled))
				}()
				// Dial a connection.
				go func() {
					defer GinkgoRecover()

					for {
						select {
						case <-ctx.Done():
							return
						default:
						}

						go func() {
							defer GinkgoRecover()

							log.Printf("re-dialing...")
							Expect(tcp.Dial(
								ctx,
								"127.0.0.1:3335",
								func(conn net.Conn) {
									log.Printf("dialed: %v", conn.RemoteAddr())
									remote := remotePrivKey.Signatory()
									enc, dec, local, err := handshake.Insecure(remote)(
										conn,
										codec.LengthPrefixEncoder(codec.PlainEncoder),
										codec.LengthPrefixDecoder(codec.PlainDecoder),
									)
									if err != nil {
										log.Printf("handshake: %v", err)
										return
									}
									if !localPrivKey.Signatory().Equal(&local) {
										log.Printf("handshake: expected %v, got %v", localPrivKey.Signatory(), local)
										return
									}
									if err := remoteCh.Attach(ctx, conn, enc, dec); err != nil {
										log.Printf("attach: %v", err)
										return
									}
								},
								func(err error) {
									log.Printf("dial: %v", err)
								},
								policy.ConstantTimeout(100*time.Millisecond),
							)).To(Succeed())
						}()

						<-time.After(1000 * time.Millisecond)
					}
				}()

				// Wait for the test to be done.
				select {
				case <-d1:
				case <-time.After(10 * time.Second):
				}

				// <-d2
			})
		})
	})
})
