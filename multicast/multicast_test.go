package multicast_test

import (
	"bytes"
	"context"
	"fmt"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/multicast"
	"github.com/renproject/aw/protocol"
	"github.com/sirupsen/logrus"
)

var _ = Describe("Multicaster", func() {
	Context("when sending multicast messages", func() {
		It("should be able to create aw compatible multicast messages", func() {
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			multicaster := NewMulticaster(messages, events, logrus.StandardLogger())
			check := func(x [32]byte) bool {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				messageBody := protocol.MessageBody(x[:])
				go func() {
					err := multicaster.Multicast(ctx, messageBody)
					if err != nil {
						fmt.Println(err)
					}
				}()
				msg := <-messages
				return (msg.To == nil) && bytes.Equal(msg.Message.Body, x[:]) && (msg.Message.Variant == protocol.Multicast)
			}
			Expect(quick.Check(check, &quick.Config{
				MaxCount: 100,
			})).Should(BeNil())
		})

		It("should return ErrCastCancelled if the context get's cancelled before multicast message is sent", func() {
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			multicaster := NewMulticaster(messages, events, logrus.StandardLogger())
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			messageBody := protocol.MessageBody{}
			err := multicaster.Multicast(ctx, messageBody)
			if err != nil {
				fmt.Println(err)
			}
			_, ok := err.(ErrMulticastingMessage)
			Expect(ok).Should(BeTrue())
		})
	})

	Context("when accepting multicast messages", func() {
		It("should be able to create aw compatible multicast messages", func() {
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			multicaster := NewMulticaster(messages, events, logrus.StandardLogger())
			check := func(x [32]byte) bool {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				messageBody := protocol.MessageBody(x[:])
				go func() {
					err := multicaster.AcceptMulticast(ctx, protocol.NewMessage(protocol.V1, protocol.Multicast, messageBody))
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

		It("should return Err if the context get's cancelled before multicast event is sent", func() {
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			multicaster := NewMulticaster(messages, events, logrus.StandardLogger())
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			messageBody := protocol.MessageBody{}
			err := multicaster.AcceptMulticast(ctx, protocol.NewMessage(protocol.V1, protocol.Multicast, messageBody))
			_, ok := err.(ErrMulticastingMessage)
			Expect(ok).Should(BeTrue())
		})

		It("should return ErrMulticastVersionNotSupported on receiving a message with invalid version", func() {
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			caster := NewMulticaster(messages, events, logrus.StandardLogger())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			message := protocol.NewMessage(protocol.V1, protocol.Multicast, protocol.MessageBody{})
			message.Version = protocol.MessageVersion(2)
			err := caster.AcceptMulticast(ctx, message)
			_, ok := err.(ErrMulticastVersionNotSupported)
			Expect(ok).Should(BeTrue())
		})

		It("should return ErrMulticastVariantNotSupported on receiving a message with invalid variant", func() {
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			caster := NewMulticaster(messages, events, logrus.StandardLogger())
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			message := protocol.NewMessage(protocol.V1, protocol.Broadcast, protocol.MessageBody{})
			err := caster.AcceptMulticast(ctx, message)
			_, ok := err.(ErrMulticastVariantNotSupported)
			Expect(ok).Should(BeTrue())
		})
	})
})
