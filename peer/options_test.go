package peer_test

import (
	"runtime"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/peer"
	. "github.com/renproject/aw/testutil"
)

var _ = Describe("options", func() {
	Context("initializing options", func() {
		It("should return an error if not providing a self PeerAddress", func() {
			option := Options{}
			Expect(option.SetZeroToDefault()).To(HaveOccurred())
		})

		It("should set unset filed to default value", func() {
			option := Options{
				Me: RandomAddress(),
			}
			Expect(option.SetZeroToDefault()).NotTo(HaveOccurred())
			Expect(option.Capacity).Should(Equal(1024))
			Expect(option.NumWorkers).Should(Equal(2 * runtime.NumCPU()))
			Expect(option.Alpha).Should(Equal(24))
			Expect(option.BootstrapDuration).Should(Equal(time.Hour))
		})
	})
})
