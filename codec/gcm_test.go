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
		It("should successfully transmit message", func() {
			var readerWriter bytes.Buffer
			data := "Hi there!"
			var key [32]byte
			rand.Read(key[:])
			privKey1 := id.NewPrivKey()
			privKey2 := id.NewPrivKey()
			gcmSession1, err := codec.NewGCMSession(key, id.NewSignatory(privKey1.PubKey()), id.NewSignatory(privKey2.PubKey()))
			Expect(err).To(BeNil())
			gcmSession2, err := codec.NewGCMSession(key, id.NewSignatory(privKey2.PubKey()), id.NewSignatory(privKey1.PubKey()))
			Expect(err).To(BeNil())

			enc := codec.GCMEncoder(gcmSession1, codec.LengthPrefixEncoder(codec.PlainEncoder))
			n, err := enc(&readerWriter, []byte(data))
			Expect(err).To(BeNil())
			Expect(n).To(Equal(9))

			var buf [4086]byte
			dec := codec.GCMDecoder(gcmSession2, codec.LengthPrefixDecoder(codec.PlainDecoder))
			n, err = dec(&readerWriter, buf[:])
			Expect(err).To(BeNil())
			Expect(n).To(Equal(9))

			Expect(string(buf[:n])).To(Equal("Hi there!"))
		})
	})
})