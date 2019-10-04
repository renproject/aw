package protocol_test

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	mrand "math/rand"
	"reflect"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/protocol"
	. "github.com/renproject/aw/testutil"
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

	Context("when processing message body", func() {
		It("should return string for valid variants", func() {
			body := [32]byte{}
			n, err := rand.Read(body[:])
			Expect(n).Should(Equal(32))
			Expect(err).NotTo(HaveOccurred())
			Expect(MessageBody(body[:]).String()).Should(Equal(base64.RawStdEncoding.EncodeToString(body[:])))
		})
	})

	Context("when processing message", func() {
		It("should create a new message with valid parameters", func() {
			body := [32]byte{}
			n, err := rand.Read(body[:])
			Expect(n).Should(Equal(32))
			Expect(err).NotTo(HaveOccurred())
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
			n, err := rand.Read(body[:])
			Expect(n).Should(Equal(32))
			Expect(err).NotTo(HaveOccurred())
			msgBody := MessageBody(body[:])
			Expect(func() { NewMessage(MessageVersion(2), Cast, msgBody) }).Should(Panic())
		})

		It("should panic for invalid variants", func() {
			body := [32]byte{}
			n, err := rand.Read(body[:])
			Expect(n).Should(Equal(32))
			Expect(err).NotTo(HaveOccurred())
			msgBody := MessageBody(body[:])
			Expect(func() { NewMessage(V1, MessageVariant(6), msgBody) }).Should(Panic())
		})

		It("should calculate the message hash of a valid message", func() {
			body := [32]byte{}
			n, err := rand.Read(body[:])
			Expect(n).Should(Equal(32))
			Expect(err).NotTo(HaveOccurred())
			msgBody := MessageBody(body[:])
			msg := NewMessage(V1, Cast, msgBody)
			Expect(func() { msg.Hash() }).ShouldNot(Panic())
		})
	})

	Context("when reading message from a io.Reader", func() {
		It("should unmarshal the bytes to a message", func() {
			test := func() bool {
				message := RandomMessage(V1, RandomMessageVariant())
				data, err := message.MarshalBinary()
				Expect(err).NotTo(HaveOccurred())
				buff := bytes.NewBuffer(data)

				msg, err := ReadMessage(buff)
				Expect(err).NotTo(HaveOccurred())

				return reflect.DeepEqual(message, msg)
			}

			Expect(quick.Check(test, nil)).Should(Succeed())
		})

		It("should return an error when the io.Reader does not have enough bytes", func() {
			test := func() bool {
				buff := bytes.NewBuffer([]byte{})
				_, err := ReadMessage(buff)
				return err != nil
			}

			Expect(quick.Check(test, nil)).Should(Succeed())
		})

		It("should return an error when the converted message has length less than 8", func() {
			test := func() bool {
				data := make([]byte, 4)
				binary.LittleEndian.PutUint32(data, uint32(mrand.Intn(8)))

				buff := bytes.NewBuffer(data)
				_, err := ReadMessage(buff)
				return err != nil
			}

			Expect(quick.Check(test, nil)).Should(Succeed())
		})
	})
})
