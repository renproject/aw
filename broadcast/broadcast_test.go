package broadcast_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing/quick"
	"time"

	"github.com/renproject/aw/protocol"
	"github.com/renproject/aw/testutil"
	"github.com/renproject/kv"
	"github.com/sirupsen/logrus"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/broadcast"
)

var _ = Describe("Broadcaster", func() {

	Context("when broadcasting", func() {
		It("should be able to send messages", func() {
			table := kv.NewTable(kv.NewMemDB(kv.JSONCodec), "airwave")
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			broadcaster := NewBroadcaster(table, messages, events, logrus.StandardLogger())

			check := func(x [32]byte) bool {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				messageBody := protocol.MessageBody(x[:])
				go func() {
					defer GinkgoRecover()
					err := broadcaster.Broadcast(ctx, messageBody)
					Expect(err).ToNot(HaveOccurred())
				}()
				message := <-messages
				return (message.To == nil) && bytes.Equal(message.Message.Body, x[:]) && (message.Message.Variant == protocol.Broadcast)
			}

			Expect(quick.Check(check, &quick.Config{
				MaxCount: 100,
			})).Should(BeNil())
		})

		Context("when the context is cancelled", func() {
			It("should return ErrBroadcasting", func() {
				table := kv.NewTable(kv.NewMemDB(kv.JSONCodec), "airwave")
				messages := make(chan protocol.MessageOnTheWire)
				events := make(chan protocol.Event)
				broadcaster := NewBroadcaster(table, messages, events, logrus.StandardLogger())

				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				err := broadcaster.Broadcast(ctx, protocol.MessageBody{})
				Expect(err.(ErrBroadcasting)).To(HaveOccurred())
			})
		})
	})

	Context("when accepting broadcasts", func() {
		It("should be able to receive messages", func() {
			table := kv.NewTable(kv.NewMemDB(kv.JSONCodec), "airwave")
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			broadcaster := NewBroadcaster(table, messages, events, logrus.StandardLogger())

			check := func(x [32]byte) bool {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				messageBody := protocol.MessageBody(x[:])
				go func() {
					defer GinkgoRecover()
					err := broadcaster.AcceptBroadcast(ctx, protocol.NewMessage(protocol.V1, protocol.Broadcast, messageBody))
					Expect(err).ToNot(HaveOccurred())
				}()
				event := (<-events).(protocol.EventMessageReceived)
				message := <-messages
				return (message.To == nil) && bytes.Equal(event.Message, x[:]) && bytes.Equal(message.Message.Body, x[:]) && (message.Message.Variant == protocol.Broadcast)
			}

			Expect(quick.Check(check, &quick.Config{
				MaxCount: 100,
			})).Should(BeNil())
		})

		Context("when the context is cancelled", func() {
			It("should return ErrAcceptingBroadcast", func() {
				table := kv.NewTable(kv.NewMemDB(kv.JSONCodec), "airwave")
				messages := make(chan protocol.MessageOnTheWire)
				events := make(chan protocol.Event)
				broadcaster := NewBroadcaster(table, messages, events, logrus.StandardLogger())

				ctx, cancel := context.WithCancel(context.Background())
				cancel()

				messageBody := protocol.MessageBody{}
				err := broadcaster.AcceptBroadcast(ctx, protocol.NewMessage(protocol.V1, protocol.Broadcast, messageBody))
				Expect(err.(ErrAcceptingBroadcast)).To(HaveOccurred())
			})
		})

		Context("when the message has an unsupported version", func() {
			It("should return ErrBroadcastVersionNotSupported", func() {
				table := kv.NewTable(kv.NewMemDB(kv.JSONCodec), "airwave")
				messages := make(chan protocol.MessageOnTheWire)
				events := make(chan protocol.Event)
				broadcaster := NewBroadcaster(table, messages, events, logrus.StandardLogger())

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				message := protocol.NewMessage(protocol.V1, protocol.Broadcast, protocol.MessageBody{})
				for message.Version == protocol.V1 {
					message.Version = protocol.MessageVersion(rand.Intn(65535))
				}
				err := broadcaster.AcceptBroadcast(ctx, message)
				Expect(err.(ErrBroadcastVersionNotSupported)).To(HaveOccurred())
			})
		})

		Context("when the message has an unsupported variant", func() {
			It("should return ErrBroadcastVariantNotSupported", func() {
				table := kv.NewTable(kv.NewMemDB(kv.JSONCodec), "airwave")
				messages := make(chan protocol.MessageOnTheWire)
				events := make(chan protocol.Event)
				broadcaster := NewBroadcaster(table, messages, events, logrus.StandardLogger())

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				message := protocol.NewMessage(protocol.V1, protocol.Broadcast, protocol.MessageBody{})
				for message.Variant == protocol.Broadcast {
					message.Variant = protocol.MessageVariant(rand.Intn(65535))
				}
				err := broadcaster.AcceptBroadcast(ctx, message)
				Expect(err.(ErrBroadcastVariantNotSupported)).To(HaveOccurred())
			})
		})

		It("should not broadcast the same message more than once", func() {
			table := kv.NewTable(kv.NewMemDB(kv.JSONCodec), "airwave")
			messages := make(chan protocol.MessageOnTheWire)
			events := make(chan protocol.Event)
			broadcaster := NewBroadcaster(table, messages, events, logrus.StandardLogger())
			check := func(x [32]byte) bool {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				messageBody := protocol.MessageBody(x[:])
				go func() {
					defer GinkgoRecover()
					err := broadcaster.AcceptBroadcast(ctx, protocol.NewMessage(protocol.V1, protocol.Broadcast, messageBody))
					Expect(err).ToNot(HaveOccurred())
				}()
				event := (<-events).(protocol.EventMessageReceived)
				if !bytes.Equal(event.Message, x[:]) {
					return false
				}
				message := <-messages
				if !((message.To == nil) && bytes.Equal(message.Message.Body, x[:]) && (message.Message.Variant == protocol.Broadcast)) {
					return false
				}

				err := broadcaster.AcceptBroadcast(ctx, protocol.NewMessage(protocol.V1, protocol.Broadcast, messageBody))
				select {
				case <-time.After(10 * time.Millisecond):
				case <-events:
					panic("broadcast the same message more than once")
				}
				return err == nil
			}
			Expect(quick.Check(check, &quick.Config{
				MaxCount: 100,
			})).Should(BeNil())
		})

		Context("when getting key/value pairs is faulty", func() {
			It("should return ErrBroadcastInternal when the table's getter is corrupted", func() {
				table := testutil.NewFaultyTable(fmt.Errorf("interal db error"), nil, nil, nil)
				messages := make(chan protocol.MessageOnTheWire)
				events := make(chan protocol.Event)
				broadcaster := NewBroadcaster(table, messages, events, logrus.StandardLogger())

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				err := broadcaster.AcceptBroadcast(ctx, protocol.NewMessage(protocol.V1, protocol.Broadcast, protocol.MessageBody{}))
				Expect(err.(ErrBroadcastInternal)).To(HaveOccurred())
			})
		})

		Context("when inserting key/value pairs is faulty", func() {
			It("should return ErrBroadcastInternal", func() {
				table := testutil.NewFaultyTable(nil, fmt.Errorf("interal db error"), nil, nil)
				messages := make(chan protocol.MessageOnTheWire)
				events := make(chan protocol.Event)
				broadcaster := NewBroadcaster(table, messages, events, logrus.StandardLogger())

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				err := broadcaster.AcceptBroadcast(ctx, protocol.NewMessage(protocol.V1, protocol.Broadcast, protocol.MessageBody{}))
				Expect(err.(ErrBroadcastInternal)).To(HaveOccurred())
			})
		})
	})
})
