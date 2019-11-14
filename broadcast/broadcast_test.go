package broadcast_test

import (
	"bytes"
	"context"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/broadcast"
	. "github.com/renproject/aw/testutil"

	"github.com/renproject/aw/protocol"
	"github.com/sirupsen/logrus"
)

var _ = Describe("Broadcaster", func() {

	Context("when broadcasting", func() {
		It("should be able to send messages", func() {
			messages := make(chan protocol.MessageOnTheWire, 128)
			events := make(chan protocol.Event, 1)
			dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
			broadcaster := NewBroadcaster(logrus.New(), messages, events, dht)

			check := func(messageBody []byte) bool {
				groupID, addrs, err := NewGroup(dht)
				Expect(err).NotTo(HaveOccurred())

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				Expect(broadcaster.Broadcast(ctx, groupID, messageBody)).NotTo(HaveOccurred())

				for i := 0; i < len(addrs); i++ {
					var message protocol.MessageOnTheWire
					Eventually(messages).Should(Receive(&message))
					Expect(addrs).Should(ContainElement(message.To))
					Expect(message.Message.Version).Should(Equal(protocol.V1))
					Expect(message.Message.Variant).Should(Equal(protocol.Broadcast))
					Expect(bytes.Equal(message.Message.Body, messageBody)).Should(BeTrue())
				}
				return true
			}

			Expect(quick.Check(check, nil)).Should(BeNil())
		})

		It("should not broadcast the same message more than once", func() {
			messages := make(chan protocol.MessageOnTheWire, 128)
			events := make(chan protocol.Event, 1)
			dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
			broadcaster := NewBroadcaster(logrus.New(), messages, events, dht)

			check := func(messageBody []byte) bool {
				groupID, addrs, err := NewGroup(dht)
				Expect(err).NotTo(HaveOccurred())

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				Expect(broadcaster.Broadcast(ctx, groupID, messageBody)).NotTo(HaveOccurred())

				for i := 0; i < len(addrs); i++ {
					var message protocol.MessageOnTheWire
					Eventually(messages).Should(Receive(&message))
					Expect(addrs).Should(ContainElement(message.To))
					Expect(message.Message.Version).Should(Equal(protocol.V1))
					Expect(message.Message.Variant).Should(Equal(protocol.Broadcast))
					Expect(bytes.Equal(message.Message.Body, messageBody)).Should(BeTrue())
				}

				Expect(broadcaster.Broadcast(ctx, groupID, messageBody)).NotTo(HaveOccurred())
				var message protocol.MessageOnTheWire
				Eventually(messages).ShouldNot(Receive(&message))
				return true
			}

			Expect(quick.Check(check, nil)).Should(BeNil())
		})

		Context("when the context is cancelled", func() {
			It("should return ErrBroadcasting", func() {
				messages := make(chan protocol.MessageOnTheWire, 128)
				events := make(chan protocol.Event, 1)
				dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
				broadcaster := NewBroadcaster(logrus.New(), messages, events, dht)

				check := func(messageBody []byte) bool {
					groupID, _, err := NewGroup(dht)
					Expect(err).NotTo(HaveOccurred())

					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					Expect(broadcaster.Broadcast(ctx, groupID, messageBody)).To(HaveOccurred())
					return true
				}

				Expect(quick.Check(check, nil)).Should(BeNil())
			})
		})

		Context("when some of the addresses cannot be found from the store", func() {
			It("should skip the nodes which we don't have the addresses", func() {
				messages := make(chan protocol.MessageOnTheWire, 128)
				events := make(chan protocol.Event, 1)
				dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
				broadcaster := NewBroadcaster(logrus.New(), messages, events, dht)

				check := func(messageBody []byte) bool {
					groupID, addrs := RandomPeerGroupID(), RandomAddresses()
					addrs = append(addrs, RandomAddress())
					Expect(dht.AddPeerGroup(groupID, FromAddressesToIDs(addrs))).NotTo(HaveOccurred())
					for i := 0; i < len(addrs)-1; i++ {
						Expect(dht.AddPeerAddress(addrs[i])).NotTo(HaveOccurred())
					}

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					Expect(broadcaster.Broadcast(ctx, groupID, messageBody)).NotTo(HaveOccurred())

					for i := 0; i < len(addrs)-1; i++ {
						var message protocol.MessageOnTheWire
						Eventually(messages).Should(Receive(&message))
						Expect(addrs).Should(ContainElement(message.To))
						Expect(message.Message.Version).Should(Equal(protocol.V1))
						Expect(message.Message.Variant).Should(Equal(protocol.Broadcast))
						Expect(bytes.Equal(message.Message.Body, messageBody)).Should(BeTrue())
					}
					Eventually(messages).ShouldNot(Receive())

					return true
				}

				Expect(quick.Check(check, nil)).Should(BeNil())
			})
		})
	})

	Context("when accepting broadcasts", func() {
		It("should be able to receive messages", func() {
			messages := make(chan protocol.MessageOnTheWire, 128)
			events := make(chan protocol.Event, 16)
			dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
			broadcaster := NewBroadcaster(logrus.New(), messages, events, dht)

			check := func(messageBody []byte) bool {
				groupID, addrs := RandomPeerGroupID(), RandomAddresses()
				for _, addr := range addrs {
					Expect(dht.AddPeerAddress(addr)).NotTo(HaveOccurred())
				}
				Expect(dht.AddPeerGroup(groupID, FromAddressesToIDs(addrs))).NotTo(HaveOccurred())

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				message := protocol.NewMessage(protocol.V1, protocol.Broadcast, groupID, messageBody)
				Expect(broadcaster.AcceptBroadcast(ctx, message)).ToNot(HaveOccurred())

				var event protocol.EventMessageReceived
				Eventually(events).Should(Receive(&event))
				Expect(bytes.Equal(event.Message, messageBody)).Should(BeTrue())

				for range addrs {
					var message protocol.MessageOnTheWire
					Eventually(messages).Should(Receive(&message))
					Expect(message.Message.Version).Should(Equal(protocol.V1))
					Expect(message.Message.Variant).Should(Equal(protocol.Broadcast))
					Expect(bytes.Equal(message.Message.Body, messageBody)).Should(BeTrue())
					Expect(addrs).Should(ContainElement(message.To))
				}
				return true
			}

			Expect(quick.Check(check, nil)).Should(BeNil())
		})

		Context("when the context is cancelled", func() {
			It("should return ErrAcceptingBroadcast", func() {
				messages := make(chan protocol.MessageOnTheWire, 128)
				events := make(chan protocol.Event, 16)
				dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
				broadcaster := NewBroadcaster(logrus.New(), messages, events, dht)

				check := func(messageBody []byte) bool {
					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					message := protocol.NewMessage(protocol.V1, protocol.Broadcast, RandomPeerGroupID(), messageBody)
					Expect(broadcaster.AcceptBroadcast(ctx, message)).To(HaveOccurred())

					return true
				}

				Expect(quick.Check(check, nil)).Should(BeNil())
			})
		})

		Context("when the message has an unsupported version", func() {
			It("should return ErrBroadcastVersionNotSupported", func() {
				messages := make(chan protocol.MessageOnTheWire, 128)
				events := make(chan protocol.Event, 16)
				dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
				broadcaster := NewBroadcaster(logrus.New(), messages, events, dht)

				check := func(messageBody []byte) bool {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					message := protocol.NewMessage(protocol.V1, protocol.Broadcast, RandomPeerGroupID(), messageBody)
					message.Version = InvalidMessageVersion()
					Expect(broadcaster.AcceptBroadcast(ctx, message)).To(HaveOccurred())

					return true
				}

				Expect(quick.Check(check, nil)).Should(BeNil())
			})
		})

		Context("when the message has an unsupported variant", func() {
			It("should return ErrBroadcastVariantNotSupported", func() {
				messages := make(chan protocol.MessageOnTheWire, 128)
				events := make(chan protocol.Event, 16)
				dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
				broadcaster := NewBroadcaster(logrus.New(), messages, events, dht)

				check := func(messageBody []byte) bool {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					message := protocol.NewMessage(protocol.V1, protocol.Broadcast, RandomPeerGroupID(), messageBody)
					message.Variant = InvalidMessageVariant()
					Expect(broadcaster.AcceptBroadcast(ctx, message)).To(HaveOccurred())

					return true
				}

				Expect(quick.Check(check, nil)).Should(BeNil())
			})
		})

		Context("when receive the same message more than once", func() {
			It("should only broadcast the same message once", func() {
				messages := make(chan protocol.MessageOnTheWire, 128)
				events := make(chan protocol.Event, 16)
				dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
				broadcaster := NewBroadcaster(logrus.New(), messages, events, dht)

				check := func(messageBody []byte) bool {
					// Intentionally not inserting the last peer address to the dht.
					groupID, addrs := RandomPeerGroupID(), RandomAddresses()
					for _, addr := range addrs {
						Expect(dht.AddPeerAddress(addr)).NotTo(HaveOccurred())
					}
					Expect(dht.AddPeerGroup(groupID, FromAddressesToIDs(addrs))).NotTo(HaveOccurred())

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					message := protocol.NewMessage(protocol.V1, protocol.Broadcast, groupID, messageBody)
					Expect(broadcaster.AcceptBroadcast(ctx, message)).ToNot(HaveOccurred())

					var event protocol.EventMessageReceived
					Eventually(events).Should(Receive(&event))
					Expect(bytes.Equal(event.Message, messageBody)).Should(BeTrue())

					for range addrs {
						var message protocol.MessageOnTheWire
						Eventually(messages).Should(Receive(&message))
						Expect(message.Message.Version).Should(Equal(protocol.V1))
						Expect(message.Message.Variant).Should(Equal(protocol.Broadcast))
						Expect(bytes.Equal(message.Message.Body, messageBody)).Should(BeTrue())
						Expect(addrs).Should(ContainElement(message.To))
					}

					Expect(broadcaster.AcceptBroadcast(ctx, message)).ToNot(HaveOccurred())
					Eventually(events).ShouldNot(Receive())
					return true
				}

				Expect(quick.Check(check, nil)).Should(BeNil())
			})
		})
	})
})
