package multicast_test

import (
	"bytes"
	"context"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/multicast"
	. "github.com/renproject/aw/testutil"

	"github.com/renproject/aw/protocol"
	"github.com/sirupsen/logrus"
)

var _ = Describe("Multicaster", func() {
	Context("when multicasting", func() {
		It("should be able to send messages", func() {
			messages := make(chan protocol.MessageOnTheWire, 128)
			events := make(chan protocol.Event, 1)
			dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
			multicaster := NewMulticaster(logrus.New(), messages, events, dht)

			check := func(messageBody []byte) bool {
				groupID, addrs, err := NewGroup(dht)
				Expect(err).NotTo(HaveOccurred())

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				Expect(multicaster.Multicast(ctx, groupID, messageBody)).NotTo(HaveOccurred())

				for i := 0; i < len(addrs); i++ {
					var message protocol.MessageOnTheWire
					Eventually(messages).Should(Receive(&message))
					Expect(addrs).Should(ContainElement(message.To))
					Expect(message.Message.Version).Should(Equal(protocol.V1))
					Expect(message.Message.Variant).Should(Equal(protocol.Multicast))
					Expect(bytes.Equal(message.Message.Body, messageBody)).Should(BeTrue())
				}
				return true
			}

			Expect(quick.Check(check, nil)).Should(BeNil())
		})

		Context("when the context is cancelled", func() {
			It("should return ErrMulticasting", func() {
				messages := make(chan protocol.MessageOnTheWire, 128)
				events := make(chan protocol.Event, 1)
				dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
				multicaster := NewMulticaster(logrus.New(), messages, events, dht)

				check := func(messageBody []byte) bool {
					groupID, _, err := NewGroup(dht)
					Expect(err).NotTo(HaveOccurred())

					ctx, cancel := context.WithCancel(context.Background())
					cancel()
					Expect(multicaster.Multicast(ctx, groupID, messageBody)).Should(HaveOccurred())
					return true
				}

				Expect(quick.Check(check, nil)).Should(BeNil())
			})
		})

		Context("when the groupID doesn't exist in dht", func() {
			It("should return an error", func() {
				messages := make(chan protocol.MessageOnTheWire, 128)
				events := make(chan protocol.Event, 1)
				dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
				multicaster := NewMulticaster(logrus.New(), messages, events, dht)

				check := func(messageBody []byte) bool {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					Expect(multicaster.Multicast(ctx, RandomPeerGroupID(), messageBody)).Should(HaveOccurred())
					return true
				}

				Expect(quick.Check(check, nil)).Should(BeNil())
			})
		})

		Context("when we don't have some addresses in the group in dht", func() {
			It("should send to all known addresses in the group and print error to warn user", func() {
				messages := make(chan protocol.MessageOnTheWire, 128)
				events := make(chan protocol.Event, 1)
				dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
				multicaster := NewMulticaster(logrus.New(), messages, events, dht)

				check := func(messageBody []byte) bool {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					groupID := RandomPeerGroupID()
					Expect(dht.AddPeerGroup(groupID, RandomPeerIDs())).NotTo(HaveOccurred())
					Expect(multicaster.Multicast(ctx, groupID, messageBody)).ShouldNot(HaveOccurred())
					return true
				}

				Expect(quick.Check(check, nil)).Should(BeNil())
			})
		})
	})

	Context("when accepting multicasts", func() {
		It("should be able to receive messages", func() {
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event, 16)
			dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
			multicaster := NewMulticaster(logrus.New(), messages, events, dht)

			check := func(messageBody []byte) bool {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				message := protocol.NewMessage(protocol.V1, protocol.Multicast, RandomPeerGroupID(), messageBody)
				Expect(multicaster.AcceptMulticast(ctx, message)).ToNot(HaveOccurred())

				var event protocol.EventMessageReceived
				Eventually(events).Should(Receive(&event))
				return bytes.Equal(event.Message, messageBody)
			}

			Expect(quick.Check(check, nil)).Should(BeNil())
		})

		Context("when the context is cancelled", func() {
			It("should return ErrAcceptingMulticast", func() {
				messages := make(chan protocol.MessageOnTheWire)
				events := make(chan protocol.Event, 16)
				dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
				multicaster := NewMulticaster(logrus.New(), messages, events, dht)

				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				message := protocol.NewMessage(protocol.V1, protocol.Multicast, RandomPeerGroupID(), protocol.MessageBody{})
				Expect(multicaster.AcceptMulticast(ctx, message)).To(HaveOccurred())
			})
		})

		Context("when the message has an unsupported version", func() {
			It("should return ErrMulticastVersionNotSupported", func() {
				messages := make(chan protocol.MessageOnTheWire)
				events := make(chan protocol.Event, 16)
				dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
				multicaster := NewMulticaster(logrus.New(), messages, events, dht)

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				message := protocol.NewMessage(protocol.V1, protocol.Multicast, RandomPeerGroupID(), protocol.MessageBody{})
				message.Version = InvalidMessageVersion()
				Expect(multicaster.AcceptMulticast(ctx, message)).To(HaveOccurred())
			})
		})

		Context("when the message has an unsupported variant", func() {
			It("should return ErrMulticastVariantNotSupported", func() {
				messages := make(chan protocol.MessageOnTheWire)
				events := make(chan protocol.Event, 16)
				dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
				multicaster := NewMulticaster(logrus.New(), messages, events, dht)

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				message := protocol.NewMessage(protocol.V1, protocol.Multicast, RandomPeerGroupID(), protocol.MessageBody{})
				message.Variant = InvalidMessageVariant(protocol.Multicast)
				Expect(multicaster.AcceptMulticast(ctx, message)).To(HaveOccurred())
			})
		})
	})
})
