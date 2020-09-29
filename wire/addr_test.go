package wire_test

import (
	"math/rand"
	"testing/quick"
	"time"

	"github.com/renproject/aw/wire"
	"github.com/renproject/aw/wire/wireutil"
	"github.com/renproject/id"
	"github.com/renproject/surge"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Address", func() {
	Context("when marshaling and unmarshaling", func() {
		It("should equal itself", func() {
			f := func() bool {
				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				addr := wireutil.NewAddressBuilder(wireutil.RandomPrivKey(), r).Build()
				data, err := surge.ToBinary(addr)
				Expect(err).ToNot(HaveOccurred())
				unmarshaledAddr := wire.Address{}
				err = surge.FromBinary(&unmarshaledAddr, data)
				Expect(err).ToNot(HaveOccurred())
				Expect(addr.Equal(&unmarshaledAddr)).To(BeTrue())
				return true
			}
			Expect(quick.Check(f, nil)).To(Succeed())
		})
	})

	Context("when stringifying and decoding", func() {
		It("should equal itself", func() {
			f := func() bool {
				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				addr := wireutil.NewAddressBuilder(wireutil.RandomPrivKey(), r).Build()
				addrString := addr.String()
				decodedAddr, err := wire.DecodeString(addrString)
				Expect(err).ToNot(HaveOccurred())
				Expect(decodedAddr.Equal(&addr)).To(BeTrue())

				// Remove the leading slash and make sure it still succeeds.
				addrString = addrString[1:]
				decodedAddr, err = wire.DecodeString(addrString)
				Expect(err).ToNot(HaveOccurred())
				Expect(decodedAddr.Equal(&addr)).To(BeTrue())
				return true
			}
			Expect(quick.Check(f, nil)).To(Succeed())
		})
	})

	Context("when recovering signature", func() {
		It("should be the correct signatory", func() {
			f := func() bool {
				pk := id.NewPrivKey()
				expectedSignatory := pk.Signatory()
				wireAddress := wire.NewUnsignedAddress(wire.TCP, "0.0.0.0", uint64(time.Now().UnixNano()))
				wireAddress.Sign(pk)
				recoveredSignatory, err := wireAddress.Signatory()
				Expect(err).ToNot(HaveOccurred())
				Expect(recoveredSignatory.Equal(&expectedSignatory)).To(BeTrue())
				return true
			}
			Expect(quick.Check(f, nil)).To(Succeed())
		})
	})

	Context("when verifying addresses", func() {
		It("should succeed for addresses with valid signatures", func() {
			f := func() bool {
				pk := id.NewPrivKey()
				expectedSignatory := pk.Signatory()
				wireAddress := wire.NewUnsignedAddress(wire.TCP, "0.0.0.0", uint64(time.Now().UnixNano()))
				wireAddress.Sign(pk)
				Expect(wireAddress.Verify(expectedSignatory)).To(Succeed())
				return true
			}
			Expect(quick.Check(f, nil)).To(Succeed())
		})

		It("should fail for addresses with invalid signatures", func() {
			f := func() bool {
				pk := id.NewPrivKey()
				expectedSignatory := pk.Signatory()
				wireAddress := wire.NewUnsignedAddress(wire.TCP, "0.0.0.0", uint64(time.Now().UnixNano()))
				rand.Read(wireAddress.Signature[:])
				// Half of the time, make sure that the recovery ID for the
				// signature is valid.
				if rand.Int()%2 == 1 {
					wireAddress.Signature[64] %= 4
				}
				Expect(wireAddress.Verify(expectedSignatory)).ToNot(Succeed())
				return true
			}
			Expect(quick.Check(f, nil)).To(Succeed())
		})
	})
})
