package multicast_test

import (
	"bytes"
	"context"
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
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			multicaster := NewMulticaster(messages, events, logrus.StandardLogger())

			check := func(x [32]byte) bool {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				messageBody := protocol.MessageBody(x[:])
				go func() {
					defer GinkgoRecover()
					err := multicaster.Multicast(ctx, messageBody)
					Expect(err).ToNot(HaveOccurred())
				}()
				message := <-messages
				return (message.To == nil) && bytes.Equal(message.Message.Body, x[:]) && (message.Message.Variant == protocol.Multicast)
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
			events := make(chan protocol.Event)
			multicaster := NewMulticaster(messages, events, logrus.StandardLogger())

			check := func(x [32]byte) bool {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				messageBody := protocol.MessageBody(x[:])
				go func() {
					defer GinkgoRecover()
					err := multicaster.AcceptMulticast(ctx, protocol.NewMessage(protocol.V1, protocol.Multicast, messageBody))
					Expect(err).ToNot(HaveOccurred())
				}()
				event := (<-events).(protocol.EventMessageReceived)
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

				err := multicaster.AcceptMulticast(ctx, protocol.NewMessage(protocol.V1, protocol.Multicast, protocol.MessageBody{}))
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
					message.Version = protocol.MessageVersion(rand.Intn(65535))
				}
				err := multicaster.AcceptMulticast(ctx, message)
				Expect(err.(ErrMulticastVersionNotSupported)).To(HaveOccurred())
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
					message.Variant = protocol.MessageVariant(rand.Intn(65535))
				}
				err := multicaster.AcceptMulticast(ctx, message)
				Expect(err.(ErrMulticastVariantNotSupported)).To(HaveOccurred())
			})
		})
	})
})
