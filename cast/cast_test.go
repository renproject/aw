package cast_test

import (
	"bytes"
	"context"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/cast"
	. "github.com/renproject/aw/testutil"

	"github.com/renproject/aw/protocol"
	"github.com/sirupsen/logrus"
)

var _ = Describe("Caster", func() {
	Context("when casting", func() {
		It("should be able to send messages", func() {
			check := func(message []byte) bool {
				messages := make(chan protocol.MessageOnTheWire, 1)
				events := make(chan protocol.Event, 1)
				me := RandomAddress()
				dht := NewDHT(me, NewTable("dht"), nil)
				caster := NewCaster(logrus.New(), messages, events, dht)

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				to := RandomAddress()
				Expect(dht.AddPeerAddress(to)).NotTo(HaveOccurred())
				Expect(caster.Cast(ctx, to.PeerID(), message)).NotTo(HaveOccurred())

				var msg protocol.MessageOnTheWire
				Eventually(messages).Should(Receive(&msg))
				Expect(msg.To.Equal(to)).Should(BeTrue())
				Expect(msg.Message.Version).Should(Equal(protocol.V1))
				Expect(msg.Message.Variant).Should(Equal(protocol.Cast))
				Expect(bytes.Equal(msg.Message.Body, message)).Should(BeTrue())

				return true
			}

			Expect(quick.Check(check, nil)).Should(BeNil())
		})

		Context("when the context is cancelled", func() {
			It("should return ErrCasting", func() {
				check := func(message []byte) bool {
					messages := make(chan protocol.MessageOnTheWire, 1)
					events := make(chan protocol.Event, 1)
					me := RandomAddress()
					dht := NewDHT(me, NewTable("dht"), nil)
					caster := NewCaster(logrus.New(), messages, events, dht)

					ctx, cancel := context.WithCancel(context.Background())
					cancel()

					to := RandomAddress()
					Expect(dht.AddPeerAddress(to)).NotTo(HaveOccurred())
					Expect(caster.Cast(ctx, to.PeerID(), message)).Should(HaveOccurred())
					return true
				}

				Expect(quick.Check(check, nil)).Should(BeNil())
			})
		})
	})

	Context("when accepting casts", func() {
		It("should be able to receive messages", func() {
			check := func(messageBody []byte) bool {
				messages := make(chan protocol.MessageOnTheWire, 1)
				events := make(chan protocol.Event, 1)
				me := RandomAddress()
				dht := NewDHT(me, NewTable("dht"), nil)
				caster := NewCaster(logrus.New(), messages, events, dht)

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				message := protocol.NewMessage(protocol.V1, protocol.Cast, protocol.NilPeerGroupID, messageBody)
				Expect(caster.AcceptCast(ctx, RandomPeerID(), message)).NotTo(HaveOccurred())

				var event protocol.EventMessageReceived
				Eventually(events).Should(Receive(&event))
				Expect(bytes.Equal(event.Message, messageBody)).Should(BeTrue())
				return true
			}

			Expect(quick.Check(check, nil)).Should(BeNil())
		})

		Context("when the context is cancelled", func() {
			It("should return ErrAcceptingCast", func() {
				check := func(messageBody []byte) bool {
					messages := make(chan protocol.MessageOnTheWire, 1)
					events := make(chan protocol.Event, 1)
					me := RandomAddress()
					dht := NewDHT(me, NewTable("dht"), nil)
					caster := NewCaster(logrus.New(), messages, events, dht)

					ctx, cancel := context.WithCancel(context.Background())
					cancel()

					message := protocol.NewMessage(protocol.V1, protocol.Cast, protocol.NilPeerGroupID, messageBody)
					Expect(caster.AcceptCast(ctx, RandomPeerID(), message)).Should(HaveOccurred())

					return true
				}

				Expect(quick.Check(check, nil)).Should(BeNil())
			})
		})

		Context("when the message has an unsupported version", func() {
			It("should return ErrCastVersionNotSupported", func() {
				check := func(messageBody []byte) bool {
					messages := make(chan protocol.MessageOnTheWire, 1)
					events := make(chan protocol.Event, 1)
					dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
					caster := NewCaster(logrus.New(), messages, events, dht)

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()

					message := protocol.NewMessage(protocol.V1, protocol.Cast, protocol.NilPeerGroupID, messageBody)
					message.Version = InvalidMessageVersion()
					Expect(caster.AcceptCast(ctx, RandomPeerID(), message)).Should(HaveOccurred())

					return true
				}

				Expect(quick.Check(check, nil)).Should(BeNil())
			})
		})

		Context("when the message has an unsupported variant", func() {
			It("should return ErrCastVariantNotSupported", func() {
				check := func(messageBody []byte) bool {
					messages := make(chan protocol.MessageOnTheWire, 1)
					events := make(chan protocol.Event, 1)
					dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
					caster := NewCaster(logrus.New(), messages, events, dht)

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()

					message := protocol.NewMessage(protocol.V1, protocol.Cast, protocol.NilPeerGroupID, messageBody)
					message.Variant = InvalidMessageVariant(protocol.Cast)

					Expect(caster.AcceptCast(ctx, RandomPeerID(), message)).Should(HaveOccurred())

					return true
				}

				Expect(quick.Check(check, nil)).Should(BeNil())
			})
		})
	})
})
