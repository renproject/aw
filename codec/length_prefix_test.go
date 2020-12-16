package codec_test

import (
	"bytes"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/renproject/aw/codec"
)

var _ = Describe("Length Prefix Codec", func() {
	Context("when encoding and decoding a message using a length prefix encoder and decoder", func() {
		It("should successfully transmit message", func() {
			var readerWriter bytes.Buffer
			data := "Hi there!"

			enc := codec.LengthPrefixEncoder(codec.PlainEncoder, codec.PlainEncoder)
			n, err := enc(&readerWriter, []byte(data))
			Expect(n).To(Equal(9))
			Expect(err).To(BeNil())

			var buf [4086]byte
			dec := codec.LengthPrefixDecoder(codec.PlainDecoder, codec.PlainDecoder)
			n, err = dec(&readerWriter, buf[:9])
			Expect(n).To(Equal(9))
			Expect(err).To(BeNil())

			Expect(string(buf[:n])).To(Equal("Hi there!"))
		})
	})

	Context("when decoding a message using a length prefix decoder with a buffer larger than the message", func() {
		It("should not return an EOF error", func() {
			var readerWriter bytes.Buffer
			data := "Hi there!"

			enc := codec.LengthPrefixEncoder(codec.PlainEncoder, codec.PlainEncoder)
			n, err := enc(&readerWriter, []byte(data))
			Expect(n).To(Equal(9))
			Expect(err).To(BeNil())

			var buf [4086]byte
			dec := codec.LengthPrefixDecoder(codec.PlainDecoder, codec.PlainDecoder)
			n, err = dec(&readerWriter, buf[:])
			Expect(n).To(Equal(9))
			Expect(err).To(BeNil())

			Expect(string(buf[:n])).To(Equal("Hi there!"))
		})
	})
})

