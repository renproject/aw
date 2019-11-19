package pingpong_test

import (
	"bytes"
	"context"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/pingpong"
	. "github.com/renproject/aw/testutil"

	"github.com/renproject/aw/protocol"
	"github.com/sirupsen/logrus"
)

var _ = Describe("Pingpong", func() {
	Context("when trying to ping another peer", func() {
		It("should send a ping message through the message sender", func() {
			test := func() bool {
				me := RandomAddress()
				messages := make(chan protocol.MessageOnTheWire, 128)
				events := make(chan protocol.Event, 1)
				dht := NewDHT(me, NewTable("dht"), nil)
				pingpong := NewPingPonger(logrus.New(), dht, messages, events, SimpleTCPPeerAddressCodec{})

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				to := RandomAddress()
				Expect(dht.AddPeerAddress(to)).NotTo(HaveOccurred())
				Expect(pingpong.Ping(ctx, to.ID)).NotTo(HaveOccurred())

				var message protocol.MessageOnTheWire
				Eventually(messages).Should(Receive(&message))
				Expect(to.Equal(message.To)).Should(BeTrue())
				Expect(message.Message.Version).Should(Equal(protocol.V1))
				Expect(message.Message.Variant).Should(Equal(protocol.Ping))

				return true
			}

			Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
		})
	})

	Context("when accepting a ping", func() {
		Context("when the address is same as before", func() {
			It("should not propogate the ping message", func() {
				test := func() bool {
					messages := make(chan protocol.MessageOnTheWire, 128)
					events := make(chan protocol.Event, 1)
					dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
					codec := SimpleTCPPeerAddressCodec{}
					pingpong := NewPingPonger(logrus.New(), dht, messages, events, codec)

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()

					sender := RandomAddress()
					Expect(dht.AddPeerAddress(sender)).NotTo(HaveOccurred())
					data, err := codec.Encode(sender)
					Expect(err).NotTo(HaveOccurred())

					ping := protocol.NewMessage(protocol.V1, protocol.Ping, protocol.NilPeerGroupID, data)
					Expect(pingpong.AcceptPing(ctx, ping)).NotTo(HaveOccurred())
					Eventually(events).ShouldNot(Receive())
					Eventually(messages).ShouldNot(Receive())
					return true
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when the address is newer than before", func() {
			It("should update its dht and propogate the message", func() {
				test := func() bool {
					messages := make(chan protocol.MessageOnTheWire, 128)
					events := make(chan protocol.Event, 1)
					bootstrapAddress := RandomAddresses()
					me := RandomAddress()
					dht := NewDHT(me, NewTable("dht"), bootstrapAddress)
					codec := SimpleTCPPeerAddressCodec{}
					pingpong := NewPingPonger(logrus.New(), dht, messages, events, codec)

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()

					sender := RandomAddress()
					data, err := codec.Encode(sender)
					Expect(err).NotTo(HaveOccurred())

					ping := protocol.NewMessage(protocol.V1, protocol.Ping, protocol.NilPeerGroupID, data)
					Expect(pingpong.AcceptPing(ctx, ping)).NotTo(HaveOccurred())

					// Expect a pong message
					var message protocol.MessageOnTheWire
					Eventually(messages).Should(Receive(&message))
					Expect(message.Message.Version).Should(Equal(protocol.V1))
					Expect(message.Message.Variant).Should(Equal(protocol.Pong))
					meData, err := codec.Encode(me)
					Expect(err).NotTo(HaveOccurred())
					Expect(bytes.Equal(message.Message.Body, meData)).Should(BeTrue())

					// Expect ping to be propagated
					for range bootstrapAddress {
						var message protocol.MessageOnTheWire
						Eventually(messages).Should(Receive(&message))
						Expect(bytes.Equal(message.Message.Body, data)).Should(BeTrue())
						Expect(message.Message.Version).Should(Equal(protocol.V1))
						Expect(message.Message.Variant).Should(Equal(protocol.Ping))
						Expect(bootstrapAddress).Should(ContainElement(message.To))
					}

					// Expect a PeerAddressChange event
					var event protocol.Event
					Eventually(events).Should(Receive(&event))
					peerChangeEvent, ok := event.(protocol.EventPeerChanged)
					Expect(ok).Should(BeTrue())
					Expect(peerChangeEvent.PeerAddress.Equal(sender)).Should(BeTrue())

					// Expect new address has been added to the dht.
					addr, err := dht.PeerAddress(sender.ID)
					Expect(err).NotTo(HaveOccurred())
					return addr.Equal(sender)
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when the message has wrong version or variant", func() {
			It("should not update the dht", func() {
				test := func() bool {
					messages := make(chan protocol.MessageOnTheWire, 128)
					events := make(chan protocol.Event, 1)
					dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
					codec := SimpleTCPPeerAddressCodec{}
					pingpong := NewPingPonger(logrus.New(), dht, messages, events, codec)

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()

					sender := RandomAddress()
					Expect(dht.AddPeerAddress(sender)).NotTo(HaveOccurred())
					data, err := codec.Encode(sender)
					Expect(err).NotTo(HaveOccurred())

					ping := protocol.NewMessage(protocol.V1, protocol.Ping, protocol.NilPeerGroupID, data)
					ping.Variant = InvalidMessageVariant(protocol.Ping)
					Expect(pingpong.AcceptPing(ctx, ping)).To(HaveOccurred())

					ping = protocol.NewMessage(protocol.V1, protocol.Ping, protocol.NilPeerGroupID, data)
					ping.Version = InvalidMessageVersion()
					Expect(pingpong.AcceptPing(ctx, ping)).To(HaveOccurred())
					return true
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})
	})

	Context("when accepting a pong", func() {
		Context("when the address is same as before", func() {
			It("should not update the dht", func() {
				test := func() bool {
					messages := make(chan protocol.MessageOnTheWire, 128)
					events := make(chan protocol.Event, 1)
					dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
					codec := SimpleTCPPeerAddressCodec{}
					pingpong := NewPingPonger(logrus.New(), dht, messages, events, codec)

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()

					sender := RandomAddress()
					Expect(dht.AddPeerAddress(sender)).NotTo(HaveOccurred())
					data, err := codec.Encode(sender)
					Expect(err).NotTo(HaveOccurred())

					pong := protocol.NewMessage(protocol.V1, protocol.Pong, protocol.NilPeerGroupID, data)
					Expect(pingpong.AcceptPong(ctx, pong)).NotTo(HaveOccurred())
					Eventually(events).ShouldNot(Receive())
					return true
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when the address is newer than before", func() {
			It("should not update the dht", func() {
				test := func() bool {
					messages := make(chan protocol.MessageOnTheWire, 128)
					events := make(chan protocol.Event, 1)
					dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
					codec := SimpleTCPPeerAddressCodec{}
					pingpong := NewPingPonger(logrus.New(), dht, messages, events, codec)

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()

					sender := RandomAddress()
					data, err := codec.Encode(sender)
					Expect(err).NotTo(HaveOccurred())

					pong := protocol.NewMessage(protocol.V1, protocol.Pong, protocol.NilPeerGroupID, data)
					Expect(pingpong.AcceptPong(ctx, pong)).NotTo(HaveOccurred())

					// Should receive EventPeerChanged event
					var event protocol.Event
					Eventually(events).Should(Receive(&event))
					peerChangeEvent, ok := event.(protocol.EventPeerChanged)
					Expect(ok).Should(BeTrue())
					Expect(peerChangeEvent.PeerAddress.Equal(sender)).Should(BeTrue())
					addr, err := dht.PeerAddress(sender.ID)
					Expect(err).NotTo(HaveOccurred())
					return addr.Equal(sender)
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when the message has wrong version or variant", func() {
			It("should not update the dht", func() {
				test := func() bool {
					messages := make(chan protocol.MessageOnTheWire, 128)
					events := make(chan protocol.Event, 1)
					dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
					codec := SimpleTCPPeerAddressCodec{}
					pingpong := NewPingPonger(logrus.New(), dht, messages, events, codec)

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()

					sender := RandomAddress()
					Expect(dht.AddPeerAddress(sender)).NotTo(HaveOccurred())
					data, err := codec.Encode(sender)
					Expect(err).NotTo(HaveOccurred())

					pong := protocol.NewMessage(protocol.V1, protocol.Pong, protocol.NilPeerGroupID, data)
					pong.Variant = InvalidMessageVariant(protocol.Pong)
					Expect(pingpong.AcceptPong(ctx, pong)).To(HaveOccurred())

					pong = protocol.NewMessage(protocol.V1, protocol.Pong, protocol.NilPeerGroupID, data)
					pong.Version = InvalidMessageVersion()
					Expect(pingpong.AcceptPong(ctx, pong)).To(HaveOccurred())
					return true
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})
	})
})
