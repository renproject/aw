package protocol_test

import (
	"bytes"
	"encoding/base64"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/protocol"
	. "github.com/renproject/aw/testutil"
)

var _ = Describe("Protocol", func() {

	Context("MessageVersion", func() {
		It("should implement the Stringer interface", func() {
			Expect(V1.String()).To(Equal("v1"))
		})

		It("should panic for invalid versions", func() {
			Expect(func() { _ = InvalidMessageVersion().String() }).To(Panic())
		})
	})

	Context("MessageVariant", func() {
		It("should implement the Stringer interface", func() {
			Expect(Ping.String()).To(Equal("ping"))
			Expect(Pong.String()).To(Equal("pong"))
			Expect(Cast.String()).To(Equal("cast"))
			Expect(Multicast.String()).To(Equal("multicast"))
			Expect(Broadcast.String()).To(Equal("broadcast"))
		})

		It("should panic for invalid variants", func() {
			Expect(func() { _ = InvalidMessageVariant().String() }).To(Panic())
		})

		It("should return the correct non-messageBody length for differernt message variant", func() {
			Expect(Ping.NonBodyLength()).To(Equal(8))
			Expect(Pong.NonBodyLength()).To(Equal(8))
			Expect(Cast.NonBodyLength()).To(Equal(8))
			Expect(Multicast.NonBodyLength()).To(Equal(40))
			Expect(Broadcast.NonBodyLength()).To(Equal(40))
		})
	})

	Context("when stringifying message bodies", func() {
		It("should return a standard raw base64 encoded string", func() {
			body := RandomMessageBody()
			decoded, err := base64.RawStdEncoding.DecodeString(body.String())
			Expect(err).NotTo(HaveOccurred())
			Expect(bytes.Equal(body, decoded)).Should(BeTrue())
		})
	})

	Context("when creating valid messages", func() {
		It("should not panic or return nil", func() {
			test := func() bool {
				variant := RandomMessageVariant()
				body := RandomMessageBody()
				Expect(NewMessage(V1, variant, NilPeerGroupID, body)).ToNot(BeNil())

				return true
			}

			Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
		})
	})

	Context("when creating invalid messages", func() {
		It("should panic for invalid versions", func() {
			messageBody := RandomMessageBody()
			Expect(func() { NewMessage(InvalidMessageVersion(), Cast, NilPeerGroupID, messageBody) }).To(Panic())
		})

		It("should panic for invalid variants", func() {
			messageBody := RandomMessageBody()
			Expect(func() { NewMessage(V1, InvalidMessageVariant(), NilPeerGroupID, messageBody) }).To(Panic())
		})

		It("should panic for invalid groupID", func() {
			messageBody := RandomMessageBody()
			Expect(func() { NewMessage(V1, Ping, RandomPeerGroupID(), messageBody) }).To(Panic())
			Expect(func() { NewMessage(V1, Pong, RandomPeerGroupID(), messageBody) }).To(Panic())
			Expect(func() { NewMessage(V1, Ping, RandomPeerGroupID(), messageBody) }).To(Panic())
		})
	})

	Context("when hashing a valid message", func() {
		It("should not panic", func() {
			message := RandomMessage(V1, RandomMessageVariant())
			hash := message.Hash()
			Expect(len(hash)).Should(Equal(32))
		})
	})

	Context("when hashing an invalid message", func() {
		It("should panic", func() {
			message := RandomMessage(V1, RandomMessageVariant())
			message.Length = 0
			Expect(func() { message.Hash() }).To(Panic())
		})
	})
})
