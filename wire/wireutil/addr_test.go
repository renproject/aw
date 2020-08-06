package wireutil_test

import (
	"math/rand"
	"testing/quick"
	"time"

	"github.com/renproject/aw/wire/wireutil"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Address builder", func() {
	Context("when building messages", func() {
		Context("when setting the protocol", func() {
			It("should build a message with that protocol", func() {
				f := func() bool {
					r := rand.New(rand.NewSource(time.Now().UnixNano()))
					protocol := wireutil.RandomAddrProtocol(r)
					addr := wireutil.NewAddressBuilder(wireutil.RandomPrivKey(), r).WithProtocol(protocol).Build()
					Expect(addr.Protocol).To(Equal(protocol))
					return true
				}
				Expect(quick.Check(f, nil)).To(Succeed())
			})
		})

		Context("when setting the value", func() {
			It("should build a message with that value", func() {
				f := func() bool {
					r := rand.New(rand.NewSource(time.Now().UnixNano()))
					value := wireutil.RandomAddrValue(r)
					addr := wireutil.NewAddressBuilder(wireutil.RandomPrivKey(), r).WithValue(value).Build()
					Expect(addr.Value).To(Equal(value))
					return true
				}
				Expect(quick.Check(f, nil)).To(Succeed())
			})
		})

		Context("when setting the nonce", func() {
			It("should build a message with that nonce", func() {
				f := func() bool {
					r := rand.New(rand.NewSource(time.Now().UnixNano()))
					nonce := wireutil.RandomAddrNonce(r)
					addr := wireutil.NewAddressBuilder(wireutil.RandomPrivKey(), r).WithNonce(nonce).Build()
					Expect(addr.Nonce).To(Equal(nonce))
					return true
				}
				Expect(quick.Check(f, nil)).To(Succeed())
			})
		})

		Context("when setting the signature", func() {
			It("should build a message with that signature", func() {
				f := func() bool {
					r := rand.New(rand.NewSource(time.Now().UnixNano()))
					signature := wireutil.RandomAddrSignature(
						wireutil.RandomOkAddrProtocol(r),
						wireutil.RandomOkAddrValue(r),
						wireutil.RandomAddrNonce(r),
						wireutil.RandomPrivKey(),
						r,
					)
					addr := wireutil.NewAddressBuilder(wireutil.RandomPrivKey(), r).WithSignature(signature).Build()
					Expect(addr.Signature.Equal(&signature)).To(BeTrue())
					return true
				}
				Expect(quick.Check(f, nil)).To(Succeed())
			})
		})
	})
})
