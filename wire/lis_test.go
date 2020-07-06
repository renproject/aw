package wire_test

import (
	"testing/quick"

	"github.com/renproject/aw/wire"
	"github.com/renproject/id"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Listener", func() {
	Context("when callbacks are nil", func() {
		It("should not panic", func() {
			f := func(version uint8, data []byte, from id.Signatory) bool {
				var err error

				cb := wire.Callbacks{}

				Expect(func() { _, err = cb.DidReceivePing(version, data, from) }).ToNot(Panic())
				Expect(err).ToNot(HaveOccurred())
				Expect(func() { err = cb.DidReceivePingAck(version, data, from) }).ToNot(Panic())
				Expect(err).ToNot(HaveOccurred())

				Expect(func() { _, err = cb.DidReceivePush(version, data, from) }).ToNot(Panic())
				Expect(err).ToNot(HaveOccurred())
				Expect(func() { err = cb.DidReceivePushAck(version, data, from) }).ToNot(Panic())
				Expect(err).ToNot(HaveOccurred())

				Expect(func() { _, err = cb.DidReceivePull(version, data, from) }).ToNot(Panic())
				Expect(err).ToNot(HaveOccurred())
				Expect(func() { err = cb.DidReceivePullAck(version, data, from) }).ToNot(Panic())
				Expect(err).ToNot(HaveOccurred())

				return true
			}
			Expect(quick.Check(f, nil)).To(Succeed())
		})
	})

	Context("when callbacks are not nil", func() {
		It("should call them", func() {
			f := func(version uint8, data []byte, from id.Signatory) bool {
				var err error

				didReceivePing := false
				didReceivePingAck := false
				didReceivePush := false
				didReceivePushAck := false
				didReceivePull := false
				didReceivePullAck := false
				cb := wire.Callbacks{
					ReceivePing: func(version uint8, data []byte, from id.Signatory) (wire.Message, error) {
						didReceivePing = true
						return wire.Message{}, nil
					},
					ReceivePingAck: func(version uint8, data []byte, from id.Signatory) error {
						didReceivePingAck = true
						return nil
					},
					ReceivePush: func(version uint8, data []byte, from id.Signatory) (wire.Message, error) {
						didReceivePush = true
						return wire.Message{}, nil
					},
					ReceivePushAck: func(version uint8, data []byte, from id.Signatory) error {
						didReceivePushAck = true
						return nil
					},
					ReceivePull: func(version uint8, data []byte, from id.Signatory) (wire.Message, error) {
						didReceivePull = true
						return wire.Message{}, nil
					},
					ReceivePullAck: func(version uint8, data []byte, from id.Signatory) error {
						didReceivePullAck = true
						return nil
					},
				}

				Expect(func() { _, err = cb.DidReceivePing(version, data, from) }).ToNot(Panic())
				Expect(didReceivePing).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
				Expect(func() { err = cb.DidReceivePingAck(version, data, from) }).ToNot(Panic())
				Expect(didReceivePingAck).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())

				Expect(func() { _, err = cb.DidReceivePush(version, data, from) }).ToNot(Panic())
				Expect(didReceivePush).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
				Expect(func() { err = cb.DidReceivePushAck(version, data, from) }).ToNot(Panic())
				Expect(didReceivePushAck).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())

				Expect(func() { _, err = cb.DidReceivePull(version, data, from) }).ToNot(Panic())
				Expect(didReceivePull).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())
				Expect(func() { err = cb.DidReceivePullAck(version, data, from) }).ToNot(Panic())
				Expect(didReceivePullAck).To(BeTrue())
				Expect(err).ToNot(HaveOccurred())

				return true
			}
			Expect(quick.Check(f, nil)).To(Succeed())
		})
	})
})
