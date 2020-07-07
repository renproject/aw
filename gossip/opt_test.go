package gossip_test

import (
	"github.com/renproject/aw/gossip"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Gossip options", func() {
	Context("when using the default options", func() {
		It("should return default values for all parameters", func() {
			opts := gossip.DefaultOptions()
			Expect(opts.Alpha).To(Equal(gossip.DefaultAlpha))
			Expect(opts.Bias).To(Equal(gossip.DefaultBias))
			Expect(opts.Timeout).To(Equal(gossip.DefaultTimeout))
			Expect(opts.MaxCapacity).To(Equal(gossip.DefaultMaxCapacity))
		})
	})
})
