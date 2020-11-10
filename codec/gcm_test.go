package codec_test

import (
	"bytes"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/renproject/aw/codec"
	"github.com/renproject/id"
	"math/rand"
)

var _ = Describe("GCM Codec", func() {
	Context("when encoding and decoding a message using a GCM encoder and decoder", func() {
		It("should successfully transmit message in both directions", func() {
			var readerWriter1 bytes.Buffer
			var readerWriter2 bytes.Buffer
			data1 := "Hi there from 1!"
			data2 := "Hi there from 2!"
			var key [32]byte
			rand.Read(key[:])
			privKey1 := id.NewPrivKey()
			privKey2 := id.NewPrivKey()
			gcmSession1, err := codec.NewGCMSession(key, id.NewSignatory(privKey1.PubKey()), id.NewSignatory(privKey2.PubKey()))
			Expect(err).To(BeNil())
			gcmSession2, err := codec.NewGCMSession(key, id.NewSignatory(privKey2.PubKey()), id.NewSignatory(privKey1.PubKey()))
			Expect(err).To(BeNil())

			enc1 := codec.GCMEncoder(gcmSession1, codec.LengthPrefixEncoder(codec.PlainEncoder))
			n1, err1 := enc1(&readerWriter1, []byte(data1))
			enc2 := codec.GCMEncoder(gcmSession2, codec.LengthPrefixEncoder(codec.PlainEncoder))
			n2, err2 := enc2(&readerWriter2, []byte(data2))
			Expect(err1).To(BeNil())
			Expect(n1).To(Equal(16))
			Expect(err2).To(BeNil())
			Expect(n2).To(Equal(16))

			var buf1 [4086]byte
			var buf2 [4086]byte
			dec1 := codec.GCMDecoder(gcmSession1, codec.LengthPrefixDecoder(codec.PlainDecoder))
			n1, err1 = dec1(&readerWriter2, buf1[:])
			dec2 := codec.GCMDecoder(gcmSession2, codec.LengthPrefixDecoder(codec.PlainDecoder))
			n2, err2 = dec2(&readerWriter1, buf2[:])
			Expect(err1).To(BeNil())
			Expect(n1).To(Equal(16))
			Expect(err2).To(BeNil())
			Expect(n2).To(Equal(16))

			Expect(string(buf1[:n1])).To(Equal("Hi there from 2!"))
			Expect(string(buf2[:n2])).To(Equal("Hi there from 1!"))

		})
	})
})
