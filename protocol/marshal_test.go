package protocol_test

import (
	"encoding/binary"
	"math"
	"math/rand"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/protocol"
	. "github.com/renproject/aw/testutil"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

var _ = Describe("marshaling and unmarshaling", func() {
	Context("message", func() {
		It("should get the same message after marshaling and unmarshaling", func() {
			test := func() bool {
				message := RandomMessage(V1, RandomMessageVariant())

				data, err := message.MarshalBinary()
				Expect(err).NotTo(HaveOccurred())

				var newMessage Message
				Expect(newMessage.UnmarshalBinary(data)).Should(Succeed())

				return cmp.Equal(message, newMessage, cmpopts.EquateEmpty())
			}

			Expect(quick.Check(test, nil)).Should(Succeed())
		})
	})

	Context("when marshaling a message", func() {
		It("should returns an error when trying to marshal a message with low length", func() {
			test := func() bool {
				message := RandomMessage(V1, RandomMessageVariant())
				message.Length = MessageLength(rand.Intn(8))
				data, err := message.MarshalBinary()
				Expect(err).To(HaveOccurred())
				Expect(data).Should(BeNil())
				return true
			}

			Expect(quick.Check(test, nil)).Should(Succeed())
		})

		It("should returns an error when trying to marshal a message with unsupported version", func() {
			test := func() bool {
				// Generate a version which is not V1.
				version := MessageVersion(rand.Intn(math.MaxUint16))
				for version == V1 {
					version = MessageVersion(rand.Intn(math.MaxUint16))
				}
				message := RandomMessage(version, RandomMessageVariant())

				data, err := message.MarshalBinary()
				Expect(err).To(HaveOccurred())
				Expect(data).Should(BeNil())
				return true
			}

			Expect(quick.Check(test, nil)).Should(Succeed())
		})

		It("should returns an error when trying to marshal a message with unsupported variant", func() {
			test := func() bool {
				// Generate a message with invalid variant
				variant := MessageVariant(rand.Intn(math.MaxUint16))
				for variant >= Ping && variant <= Broadcast {
					variant = MessageVariant(rand.Intn(math.MaxUint16))
				}
				message := RandomMessage(V1, variant)

				data, err := message.MarshalBinary()
				Expect(err).To(HaveOccurred())
				Expect(data).Should(BeNil())
				return true
			}

			Expect(quick.Check(test, nil)).Should(Succeed())
		})
	})

	Context("when unmarshaling a message", func() {
		It("should return EOF when the data does not have enough bytes for uint32", func() {
			test := func() bool {
				data := RandomBytes(3)

				var message Message
				err := message.UnmarshalBinary(data)
				Expect(err).ShouldNot(BeNil())
				return true
			}

			Expect(quick.Check(test, nil)).Should(Succeed())
		})

		It("should return an error when the message length is less than 8", func() {
			test := func() bool {
				data := make([]byte, 4)
				binary.LittleEndian.PutUint32(data, uint32(rand.Intn(8)))

				var message Message
				err := message.UnmarshalBinary(data)
				Expect(err).ShouldNot(BeNil())
				return true
			}

			Expect(quick.Check(test, nil)).Should(Succeed())
		})
	})
})
