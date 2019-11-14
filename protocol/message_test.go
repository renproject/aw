package protocol_test

import (
	"crypto/rand"
	"encoding/base64"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/protocol"
)

var _ = Describe("Protocol", func() {
	Context("when checking message versions", func() {
		It("should return a version string", func() {
			Expect(V1.String()).To(Equal("v1"))
		})

		It("should panic for invalid versions", func() {
			Expect(func() { _ = MessageVersion(2).String() }).To(Panic())
		})
	})

	Context("when checking message variants", func() {
		It("should return a variant string", func() {
			Expect(Ping.String()).To(Equal("ping"))
			Expect(Pong.String()).To(Equal("pong"))
			Expect(Cast.String()).To(Equal("cast"))
			Expect(Multicast.String()).To(Equal("multicast"))
			Expect(Broadcast.String()).To(Equal("broadcast"))
		})

		It("should panic for invalid variants", func() {
			Expect(func() { _ = MessageVariant(6).String() }).To(Panic())
		})
	})

	Context("when stringifying message bodies", func() {
		It("should return a base64 string", func() {
			body := [32]byte{}
			n, err := rand.Read(body[:])
			Expect(n).To(Equal(32))
			Expect(err).ToNot(HaveOccurred())
			Expect(MessageBody(body[:]).String()).To(Equal(base64.RawStdEncoding.EncodeToString(body[:])))
		})
	})

	Context("when creating valid messages", func() {
		It("should not return nil", func() {
			body := [32]byte{}
			n, err := rand.Read(body[:])
			Expect(n).To(Equal(32))
			Expect(err).ToNot(HaveOccurred())
			messageBody := MessageBody(body[:])
			Expect(NewMessage(V1, Cast, NilPeerGroupID, messageBody)).ToNot(BeNil())
			Expect(NewMessage(V1, Multicast, NilPeerGroupID, messageBody)).ToNot(BeNil())
			Expect(NewMessage(V1, Broadcast, NilPeerGroupID, messageBody)).ToNot(BeNil())
			Expect(NewMessage(V1, Ping, NilPeerGroupID, messageBody)).ToNot(BeNil())
			Expect(NewMessage(V1, Pong, NilPeerGroupID, messageBody)).ToNot(BeNil())
		})
	})

	Context("when creating valid empty messages", func() {
		It("should not return nil", func() {
			Expect(NewMessage(V1, Cast, NilPeerGroupID, nil)).ToNot(BeNil())
			Expect(NewMessage(V1, Multicast, NilPeerGroupID, nil)).ToNot(BeNil())
			Expect(NewMessage(V1, Broadcast, NilPeerGroupID, nil)).ToNot(BeNil())
			Expect(NewMessage(V1, Ping, NilPeerGroupID, nil)).ToNot(BeNil())
			Expect(NewMessage(V1, Pong, NilPeerGroupID, nil)).ToNot(BeNil())
		})
	})

	Context("when creating invalid messages", func() {
		It("should panic for invalid versions", func() {
			body := [32]byte{}
			n, err := rand.Read(body[:])
			Expect(n).To(Equal(32))
			Expect(err).ToNot(HaveOccurred())
			messageBody := MessageBody(body[:])
			Expect(func() { NewMessage(MessageVersion(2), Cast, NilPeerGroupID, messageBody) }).To(Panic())
		})

		It("should panic for invalid variants", func() {
			body := [32]byte{}
			n, err := rand.Read(body[:])
			Expect(n).To(Equal(32))
			Expect(err).ToNot(HaveOccurred())
			messageBody := MessageBody(body[:])
			Expect(func() { NewMessage(V1, MessageVariant(6), NilPeerGroupID, messageBody) }).To(Panic())
		})

	})

	Context("when hashing a valid message", func() {
		It("should not panic", func() {
			body := [32]byte{}
			n, err := rand.Read(body[:])
			Expect(n).To(Equal(32))
			Expect(err).ToNot(HaveOccurred())
			messageBody := MessageBody(body[:])
			message := NewMessage(V1, Cast, NilPeerGroupID, messageBody)
			Expect(func() { message.Hash() }).ToNot(Panic())
		})
	})

	Context("when hashing an invalid message", func() {
		It("should panic", func() {
			body := [32]byte{}
			n, err := rand.Read(body[:])
			Expect(n).To(Equal(32))
			Expect(err).ToNot(HaveOccurred())
			messageBody := MessageBody(body[:])
			message := NewMessage(V1, Cast, NilPeerGroupID, messageBody)
			message.Length = 0
			Expect(func() { message.Hash() }).To(Panic())
		})
	})
})
