package wire_test

import (
	"math/rand"

	"github.com/renproject/aw/wire"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Address", func() {
	Context("when generating an address hash", func() {
		It("should generate a different hash for a different address", func() {
			r := rand.New(rand.NewSource(GinkgoRandomSeed()))
			addr := wire.NewUnsignedAddress(wire.TCP, "", 0)

			// Generate a hash for the address.
			h1, err := wire.NewAddressHash(addr.Protocol, addr.Value, addr.Nonce)
			Expect(err).ToNot(HaveOccurred())

			// Generate a new address hash with a different protocol.
			h2, err := wire.NewAddressHash(wire.Protocol(r.Uint32()), addr.Value, addr.Nonce)
			Expect(err).ToNot(HaveOccurred())

			// Generate a new address hash with a different value.
			b := make([]byte, 100)
			_, err = rand.Read(b)
			Expect(err).ToNot(HaveOccurred())
			h3, err := wire.NewAddressHash(addr.Protocol, string(b), addr.Nonce)
			Expect(err).ToNot(HaveOccurred())

			// Generate a new address hash with a different nonce.
			h4, err := wire.NewAddressHash(addr.Protocol, addr.Value, r.Uint64())
			Expect(err).ToNot(HaveOccurred())

			// Ensure all address hashes are different.
			Expect(h1).ToNot(Equal(h2))
			Expect(h1).ToNot(Equal(h3))
			Expect(h1).ToNot(Equal(h4))
			Expect(h2).ToNot(Equal(h3))
			Expect(h2).ToNot(Equal(h4))
			Expect(h3).ToNot(Equal(h4))
		})
	})
})
