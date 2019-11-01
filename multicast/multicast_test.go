package multicast_test

import (
	"bytes"
	"context"
	"math"
	"math/rand"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/multicast"

	"github.com/renproject/aw/protocol"
	"github.com/sirupsen/logrus"
)

var _ = Describe("Multicaster", func() {
	Context("when multicasting", func() {
		It("should be able to send messages", func() {
			messages := make(chan protocol.MessageOnTheWire, 16)
			events := make(chan protocol.Event)
			multicaster := NewMulticaster(messages, events, logrus.StandardLogger())

			check := func(x [32]byte) bool {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				messageBody := protocol.MessageBody(x[:])
				err := multicaster.Multicast(ctx, messageBody)
				Expect(err).ToNot(HaveOccurred())

				message := <-messages
				Expect(message.To).Should(BeNil())
				Expect(message.Message.Version).Should(Equal(protocol.V1))
				Expect(message.Message.Variant).Should(Equal(protocol.Multicast))
				Expect(bytes.Equal(message.Message.Body, x[:])).Should(BeTrue())
				return true
			}

			Expect(quick.Check(check, &quick.Config{
				MaxCount: 100,
			})).Should(BeNil())
		})

		Context("when the context is cancelled", func() {
			It("should return ErrMulticasting", func() {
				messages := make(chan protocol.MessageOnTheWire)
				events := make(chan protocol.Event)
				multicaster := NewMulticaster(messages, events, logrus.StandardLogger())

				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				err := multicaster.Multicast(ctx, protocol.MessageBody{})
				Expect(err.(ErrMulticasting)).To(HaveOccurred())
			})
		})
	})

	Context("when accepting multicasts", func() {
		It("should be able to receive messages", func() {
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event,16)
			multicaster := NewMulticaster(messages, events, logrus.StandardLogger())

			check := func(x [32]byte) bool {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				messageBody := protocol.MessageBody(x[:])
				message := protocol.NewMessage(protocol.V1, protocol.Multicast, messageBody)
				Expect(multicaster.AcceptMulticast(ctx, message)).ToNot(HaveOccurred())

				event, ok := (<-events).(protocol.EventMessageReceived)
				Expect(ok).Should(BeTrue())
				return bytes.Equal(event.Message, x[:])
			}

			Expect(quick.Check(check, &quick.Config{
				MaxCount: 100,
			})).Should(BeNil())
		})

		Context("when the context is cancelled", func() {
			It("should return ErrAcceptingMulticast", func() {
				messages := make(chan protocol.MessageOnTheWire)
				events := make(chan protocol.Event)
				multicaster := NewMulticaster(messages, events, logrus.StandardLogger())

				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				message := protocol.NewMessage(protocol.V1, protocol.Multicast, protocol.MessageBody{})
				err := multicaster.AcceptMulticast(ctx, message)
				Expect(err.(ErrAcceptingMulticast)).To(HaveOccurred())
			})
		})

		Context("when the message has an unsupported version", func() {
			It("should return ErrMulticastVersionNotSupported", func() {
				messages := make(chan protocol.MessageOnTheWire)
				events := make(chan protocol.Event)
				multicaster := NewMulticaster(messages, events, logrus.StandardLogger())

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				message := protocol.NewMessage(protocol.V1, protocol.Multicast, protocol.MessageBody{})
				for message.Version == protocol.V1 {
					message.Version = protocol.MessageVersion(rand.Intn(math.MaxUint16))
				}
				err := multicaster.AcceptMulticast(ctx, message)
				Expect(err.(protocol.ErrMessageVersionIsNotSupported)).To(HaveOccurred())
			})
		})

		Context("when the message has an unsupported variant", func() {
			It("should return ErrMulticastVariantNotSupported", func() {
				messages := make(chan protocol.MessageOnTheWire)
				events := make(chan protocol.Event)
				multicaster := NewMulticaster(messages, events, logrus.StandardLogger())

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				message := protocol.NewMessage(protocol.V1, protocol.Multicast, protocol.MessageBody{})
				for message.Variant == protocol.Multicast {
					message.Variant = protocol.MessageVariant(rand.Intn(math.MaxUint16))
				}
				err := multicaster.AcceptMulticast(ctx, message)
				Expect(err.(protocol.ErrMessageVariantIsNotSupported)).To(HaveOccurred())
			})
		})
	})
})
