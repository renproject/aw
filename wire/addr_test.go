package wire_test

import (
	"math/rand"
	"testing/quick"
	"time"

	"github.com/renproject/aw/wire"
	"github.com/renproject/aw/wire/wireutil"
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
})
