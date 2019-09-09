package broadcast_test

import (
	"bytes"
	"context"
	"fmt"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/broadcast"
	"github.com/renproject/aw/protocol"
	"github.com/renproject/aw/testutil"
	"github.com/renproject/kv"
	"github.com/sirupsen/logrus"
)

var _ = Describe("Broadcaster", func() {
	Context("when sending broadcast messages", func() {
		It("should be able to create aw compatible broadacast messages", func() {
			table := kv.NewTable(kv.NewMemDB(kv.JSONCodec), "broadcaster")
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			broadcaster := NewBroadcaster(table, messages, events, logrus.StandardLogger())
			check := func(x [32]byte) bool {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				messageBody := protocol.MessageBody(x[:])
				go func() {
					err := broadcaster.Broadcast(ctx, messageBody)
					if err != nil {
						fmt.Println(err)
					}
				}()
				msg := <-messages
				return (msg.To == nil) && bytes.Equal(msg.Message.Body, x[:]) && (msg.Message.Variant == protocol.Broadcast)
			}
			Expect(quick.Check(check, &quick.Config{
				MaxCount: 100,
			})).Should(BeNil())
		})

		It("should return ErrBroadcastCanceled if the context get's cancelled before broadcast message is sent", func() {
			table := kv.NewTable(kv.NewMemDB(kv.JSONCodec), "broadcaster")
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			broadcaster := NewBroadcaster(table, messages, events, logrus.StandardLogger())
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			messageBody := protocol.MessageBody{}
			err := broadcaster.Broadcast(ctx, messageBody)
			if err != nil {
				fmt.Println(err)
			}
			_, ok := err.(ErrBroadcastCanceled)
			Expect(ok).Should(BeTrue())
		})
	})

	Context("when accepting broadcast messages", func() {
		It("should be able to create aw compatible broadacast messages", func() {
			table := kv.NewTable(kv.NewMemDB(kv.JSONCodec), "broadcaster")
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			broadcaster := NewBroadcaster(table, messages, events, logrus.StandardLogger())
			check := func(x [32]byte) bool {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				messageBody := protocol.MessageBody(x[:])
				go func() {
					err := broadcaster.AcceptBroadcast(ctx, protocol.NewMessage(protocol.V1, protocol.Broadcast, messageBody))
					if err != nil {
						fmt.Println(err)
					}
				}()
				ev := <-events
				event, ok := ev.(protocol.EventMessageReceived)
				if !ok {
					return false
				}
				if !bytes.Equal(event.Message, x[:]) {
					return false
				}
				msg := <-messages
				return (msg.To == nil) && bytes.Equal(msg.Message.Body, x[:]) && (msg.Message.Variant == protocol.Broadcast)
			}
			Expect(quick.Check(check, &quick.Config{
				MaxCount: 100,
			})).Should(BeNil())
		})

		It("should return ErrBroadcastCanceled if the context get's cancelled before broadcast event is sent", func() {
			table := kv.NewTable(kv.NewMemDB(kv.JSONCodec), "broadcaster")
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			broadcaster := NewBroadcaster(table, messages, events, logrus.StandardLogger())
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			messageBody := protocol.MessageBody{}
			err := broadcaster.AcceptBroadcast(ctx, protocol.NewMessage(protocol.V1, protocol.Broadcast, messageBody))
			_, ok := err.(ErrBroadcastCanceled)
			Expect(ok).Should(BeTrue())
		})

		It("should return ErrBroadcastVersionNotSupported when trying to accept a message with unsupported version", func() {
			table := kv.NewTable(kv.NewMemDB(kv.JSONCodec), "broadcaster")
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			broadcaster := NewBroadcaster(table, messages, events, logrus.StandardLogger())
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			msg := protocol.NewMessage(protocol.V1, protocol.Broadcast, protocol.MessageBody{})
			msg.Version = protocol.MessageVersion(2)
			err := broadcaster.AcceptBroadcast(ctx, msg)
			Expect(err).ShouldNot(BeNil())
			_, ok := err.(ErrBroadcastVersionNotSupported)
			Expect(ok).Should(BeTrue())
		})

		It("should return ErrBroadcastVariantNotSupported when trying to accept a message with a wrong variant", func() {
			table := kv.NewTable(kv.NewMemDB(kv.JSONCodec), "broadcaster")
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			broadcaster := NewBroadcaster(table, messages, events, logrus.StandardLogger())
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			messageBody := protocol.MessageBody{}
			err := broadcaster.AcceptBroadcast(ctx, protocol.NewMessage(protocol.V1, protocol.Cast, messageBody))
			Expect(err).ShouldNot(BeNil())
			_, ok := err.(ErrBroadcastVariantNotSupported)
			Expect(ok).Should(BeTrue())
		})

		It("should not broadcast the same message twice", func() {
			table := kv.NewTable(kv.NewMemDB(kv.JSONCodec), "broadcaster")
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			broadcaster := NewBroadcaster(table, messages, events, logrus.StandardLogger())
			check := func(x [32]byte) bool {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				messageBody := protocol.MessageBody(x[:])
				go func() {
					err := broadcaster.AcceptBroadcast(ctx, protocol.NewMessage(protocol.V1, protocol.Broadcast, messageBody))
					if err != nil {
						fmt.Println(err)
					}
				}()
				ev := <-events
				event, ok := ev.(protocol.EventMessageReceived)
				if !ok {
					return false
				}
				if !bytes.Equal(event.Message, x[:]) {
					return false
				}
				msg := <-messages
				if !((msg.To == nil) && bytes.Equal(msg.Message.Body, x[:]) && (msg.Message.Variant == protocol.Broadcast)) {
					return false
				}
				err := broadcaster.AcceptBroadcast(ctx, protocol.NewMessage(protocol.V1, protocol.Broadcast, messageBody))
				return err == nil
			}
			Expect(quick.Check(check, &quick.Config{
				MaxCount: 100,
			})).Should(BeNil())
		})

		It("should return ErrBroadcastInternal when the table's getter is corrupted", func() {
			table := testutil.NewFaultyTable(fmt.Errorf("interal db error"), nil, nil, nil)
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			broadcaster := NewBroadcaster(table, messages, events, logrus.StandardLogger())
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			messageBody := protocol.MessageBody{}
			err := broadcaster.AcceptBroadcast(ctx, protocol.NewMessage(protocol.V1, protocol.Broadcast, messageBody))
			Expect(err).ShouldNot(BeNil())
			_, ok := err.(ErrBroadcastInternal)
			Expect(ok).Should(BeTrue())
		})

		It("should return ErrBroadcastInternal when the table's inserter is corrupted", func() {
			table := testutil.NewFaultyTable(nil, fmt.Errorf("interal db error"), nil, nil)
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			broadcaster := NewBroadcaster(table, messages, events, logrus.StandardLogger())
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			messageBody := protocol.MessageBody{}
			err := broadcaster.AcceptBroadcast(ctx, protocol.NewMessage(protocol.V1, protocol.Broadcast, messageBody))
			Expect(err).ShouldNot(BeNil())
			_, ok := err.(ErrBroadcastInternal)
			Expect(ok).Should(BeTrue())
		})
	})
})
