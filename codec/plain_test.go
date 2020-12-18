package codec_test

import (
	"bytes"
	"github.com/renproject/aw/codec"
	"io"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Plain Codec", func() {
	Context("when encoding and decoding a message using a plain encoder and decoder", func() {
		It("should successfully transmit message", func() {
			var readerWriter bytes.Buffer
			data := "Hi there!"

			n, err := codec.PlainEncoder(&readerWriter, []byte(data))
			Expect(n).To(Equal(9))
			Expect(err).To(BeNil())

			var buf [4086]byte
			n, err = codec.PlainDecoder(&readerWriter, buf[:9])
			Expect(n).To(Equal(9))
			Expect(err).To(BeNil())

			Expect(string(buf[:n])).To(Equal("Hi there!"))
		})
	})

	Context("when decoding a message using a plain decoder with a buffer larger than the message", func() {
		It("should return an EOF error", func() {
			var readerWriter bytes.Buffer
			data := "Hi there!"

			n, err := codec.PlainEncoder(&readerWriter, []byte(data))
			Expect(n).To(Equal(9))
			Expect(err).To(BeNil())

			var buf [4086]byte
			n, err = codec.PlainDecoder(&readerWriter, buf[:])
			Expect(n).To(Equal(9))
			Expect(err).To(Equal(io.ErrUnexpectedEOF))

			Expect(string(buf[:n])).To(Equal("Hi there!"))
		})
	})
})