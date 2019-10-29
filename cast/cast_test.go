package cast_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"math/rand"
	"testing/quick"

	"github.com/renproject/aw/protocol"
	"github.com/renproject/aw/testutil"
	"github.com/sirupsen/logrus"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/cast"
)

var _ = Describe("Caster", func() {
	Context("when casting", func() {
		It("should be able send messages", func() {
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			caster := NewCaster(messages, events, logrus.StandardLogger())

			check := func(x, y [32]byte) bool {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				messageBody := protocol.MessageBody(x[:])
				to := testutil.SimplePeerID(hex.EncodeToString(y[:]))
				go func() {
					defer GinkgoRecover()
					err := caster.Cast(ctx, to, messageBody)
					Expect(err).ToNot(HaveOccurred())
				}()
				message := <-messages

				return (message.To.Equal(to)) && bytes.Equal(message.Message.Body, x[:]) && (message.Message.Variant == protocol.Cast)
			}

			Expect(quick.Check(check, &quick.Config{
				MaxCount: 100,
			})).Should(BeNil())
		})

		Context("when the context is cancelled", func() {
			It("should return ErrCasting", func() {
				messages := make(chan protocol.MessageOnTheWire)
				events := make(chan protocol.Event)
				caster := NewCaster(messages, events, logrus.StandardLogger())

				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				to := testutil.SimplePeerID("")
				err := caster.Cast(ctx, to, protocol.MessageBody{})
				Expect(err.(ErrCasting)).To(HaveOccurred())
			})
		})
	})

	Context("when accepting casts", func() {
		It("should be able to receive messages", func() {
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			caster := NewCaster(messages, events, logrus.StandardLogger())

			check := func(x, y [32]byte) bool {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				messageBody := protocol.MessageBody(x[:])
				go func() {
					defer GinkgoRecover()
					err := caster.AcceptCast(ctx, protocol.NewMessage(protocol.V1, protocol.Cast, messageBody))
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
			It("should return ErrAcceptingCast", func() {
				messages := make(chan protocol.MessageOnTheWire)
				events := make(chan protocol.Event)
				caster := NewCaster(messages, events, logrus.StandardLogger())

				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				err := caster.AcceptCast(ctx, protocol.NewMessage(protocol.V1, protocol.Cast, protocol.MessageBody{}))
				Expect(err.(ErrAcceptingCast)).To(HaveOccurred())
			})
		})

		Context("when the message has an unsupported version", func() {
			It("should return ErrCastVersionNotSupported on receiving a message with invalid version", func() {
				messages := make(chan protocol.MessageOnTheWire)
				events := make(chan protocol.Event)
				caster := NewCaster(messages, events, logrus.StandardLogger())

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				message := protocol.NewMessage(protocol.V1, protocol.Cast, protocol.MessageBody{})
				for message.Version == protocol.V1 {
					message.Version = protocol.MessageVersion(rand.Intn(65535))
				}
				err := caster.AcceptCast(ctx, message)
				Expect(err.(ErrCastVersionNotSupported)).To(HaveOccurred())
			})
		})

		Context("when the message has an unsupported variant", func() {
			It("should return ErrCastVariantNotSupported on receiving a message with invalid variant", func() {
				messages := make(chan protocol.MessageOnTheWire)
				events := make(chan protocol.Event)
				caster := NewCaster(messages, events, logrus.StandardLogger())

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				message := protocol.NewMessage(protocol.V1, protocol.Cast, protocol.MessageBody{})
				for message.Variant == protocol.Cast {
					message.Variant = protocol.MessageVariant(rand.Intn(65535))
				}
				err := caster.AcceptCast(ctx, message)
				Expect(err.(ErrCastVariantNotSupported)).To(HaveOccurred())
			})
		})
	})
})
