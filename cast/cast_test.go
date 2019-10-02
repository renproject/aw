package cast_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/cast"

	"github.com/renproject/aw/protocol"
	"github.com/renproject/aw/testutil"
	"github.com/sirupsen/logrus"
)

var _ = Describe("Caster", func() {
	Context("when sending cast messages", func() {
		It("should be able to create aw compatible cast messages", func() {
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			caster := NewCaster(messages, events, logrus.StandardLogger())
			check := func(x, y [32]byte) bool {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				messageBody := protocol.MessageBody(x[:])
				to := testutil.SimplePeerID(hex.EncodeToString(y[:]))
				go func() {
					err := caster.Cast(ctx, to, messageBody)
					if err != nil {
						fmt.Println(err)
					}
				}()
				msg := <-messages
				return (msg.To.Equal(to)) && bytes.Equal(msg.Message.Body, x[:]) && (msg.Message.Variant == protocol.Cast)
			}
			Expect(quick.Check(check, &quick.Config{
				MaxCount: 100,
			})).Should(BeNil())
		})

		It("should return ErrCastingMessage if the context get's cancelled before cast message is sent", func() {
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			caster := NewCaster(messages, events, logrus.StandardLogger())
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			messageBody := protocol.MessageBody{}
			to := testutil.SimplePeerID("")
			err := caster.Cast(ctx, to, messageBody)
			if err != nil {
				fmt.Println(err)
			}
			_, ok := err.(ErrCastingMessage)
			Expect(ok).Should(BeTrue())
		})
	})

	Context("when accepting cast messages", func() {
		It("should be able to create aw compatible cast messages", func() {
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			caster := NewCaster(messages, events, logrus.StandardLogger())
			check := func(x, y [32]byte) bool {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				messageBody := protocol.MessageBody(x[:])
				go func() {
					err := caster.AcceptCast(ctx, protocol.NewMessage(protocol.V1, protocol.Cast, messageBody))
					if err != nil {
						fmt.Println(err)
					}
				}()
				ev := <-events
				event, ok := ev.(protocol.EventMessageReceived)
				if !ok {
					return false
				}
				return bytes.Equal(event.Message, x[:])
			}
			Expect(quick.Check(check, &quick.Config{
				MaxCount: 100,
			})).Should(BeNil())
		})

		It("should return ErrAcceptCastingMessage if the context get's cancelled before cast event is sent", func() {
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			caster := NewCaster(messages, events, logrus.StandardLogger())
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			messageBody := protocol.MessageBody{}
			err := caster.AcceptCast(ctx, protocol.NewMessage(protocol.V1, protocol.Cast, messageBody))
			_, ok := err.(ErrAcceptCastingMessage)
			Expect(ok).Should(BeTrue())
		})

		It("should return ErrCastVersionNotSupported on receiving a message with invalid version", func() {
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			caster := NewCaster(messages, events, logrus.StandardLogger())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			message := protocol.NewMessage(protocol.V1, protocol.Cast, protocol.MessageBody{})
			message.Version = protocol.MessageVersion(2)
			err := caster.AcceptCast(ctx, message)
			_, ok := err.(ErrCastVersionNotSupported)
			Expect(ok).Should(BeTrue())
		})

		It("should return ErrCastVariantNotSupported on receiving a message with invalid variant", func() {
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			caster := NewCaster(messages, events, logrus.StandardLogger())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			message := protocol.NewMessage(protocol.V1, protocol.Multicast, protocol.MessageBody{})
			err := caster.AcceptCast(ctx, message)
			_, ok := err.(ErrCastVariantNotSupported)
			Expect(ok).Should(BeTrue())
		})
	})
})
