package protocol_test

import (
	"crypto/rand"
	"encoding/base64"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/protocol"
)

var _ = Describe("Protocol", func() {
	Context("when processing message versions", func() {
		It("should return version string", func() {
			Expect(V1.String()).Should(Equal("v1"))
		})

		It("should panic for invalid versions", func() {
			Expect(func() { _ = MessageVersion(2).String() }).Should(Panic())
		})
	})

	Context("when processing message variants", func() {
		It("should return string for valid variants", func() {
			Expect(Ping.String()).Should(Equal("ping"))
			Expect(Pong.String()).Should(Equal("pong"))
			Expect(Cast.String()).Should(Equal("cast"))
			Expect(Multicast.String()).Should(Equal("multicast"))
			Expect(Broadcast.String()).Should(Equal("broadcast"))
		})

		It("should panic for invalid variants", func() {
			Expect(func() { _ = MessageVariant(6).String() }).Should(Panic())
		})
	})

	Context("when processing message hashes", func() {
		It("should return base64 string for valid message hashes", func() {
			hash := MessageHash{}
			rand.Read(hash[:])
			Expect(hash.String()).Should(Equal(base64.StdEncoding.EncodeToString(hash[:])))
		})
	})

	Context("when processing message body", func() {
		It("should return string for valid variants", func() {
			body := [32]byte{}
			rand.Read(body[:])
			Expect(MessageBody(body[:]).String()).Should(Equal(base64.StdEncoding.EncodeToString(body[:])))
		})
	})

	Context("when processing message", func() {
		It("should create a new message with valid parameters", func() {
			body := [32]byte{}
			rand.Read(body[:])
			msgBody := MessageBody(body[:])
			Expect(NewMessage(V1, Cast, msgBody)).ShouldNot(BeNil())
			Expect(NewMessage(V1, Multicast, msgBody)).ShouldNot(BeNil())
			Expect(NewMessage(V1, Broadcast, msgBody)).ShouldNot(BeNil())
			Expect(NewMessage(V1, Ping, msgBody)).ShouldNot(BeNil())
			Expect(NewMessage(V1, Pong, msgBody)).ShouldNot(BeNil())
		})

		It("should create a new message with valid parameters and nil body", func() {
			Expect(NewMessage(V1, Cast, nil)).ShouldNot(BeNil())
			Expect(NewMessage(V1, Multicast, nil)).ShouldNot(BeNil())
			Expect(NewMessage(V1, Broadcast, nil)).ShouldNot(BeNil())
			Expect(NewMessage(V1, Ping, nil)).ShouldNot(BeNil())
			Expect(NewMessage(V1, Pong, nil)).ShouldNot(BeNil())
		})

		It("should panic for invalid versions", func() {
			body := [32]byte{}
			rand.Read(body[:])
			msgBody := MessageBody(body[:])
			Expect(func() { NewMessage(MessageVersion(2), Cast, msgBody) }).Should(Panic())
		})

		It("should panic for invalid variants", func() {
			body := [32]byte{}
			rand.Read(body[:])
			msgBody := MessageBody(body[:])
			Expect(func() { NewMessage(V1, MessageVariant(6), msgBody) }).Should(Panic())
		})

		It("should calculate the message hash of a valid message", func() {
			body := [32]byte{}
			rand.Read(body[:])
			msgBody := MessageBody(body[:])
			msg := NewMessage(V1, Cast, msgBody)
			Expect(func() { msg.Hash() }).ShouldNot(Panic())
		})
	})
})
