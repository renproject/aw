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

var _ = Describe("Message", func() {
	Context("when marshaling and unmarshaling", func() {
		It("shoudl equal itself", func() {
			f := func() bool {
				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				msg := wireutil.NewMessageBuilder(r).Build()
				data, err := surge.ToBinary(msg)
				Expect(err).ToNot(HaveOccurred())
				unmarshaledMsg := wire.Message{}
				err = surge.FromBinary(&unmarshaledMsg, data)
				Expect(err).ToNot(HaveOccurred())
				Expect(msg.Equal(&unmarshaledMsg)).To(BeTrue())
				return true
			}
			Expect(quick.Check(f, nil)).To(Succeed())
		})
	})
})
