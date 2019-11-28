package peer_test

import (
	"runtime"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/peer"
	. "github.com/renproject/aw/testutil"

	"github.com/sirupsen/logrus"
)

var _ = Describe("options", func() {
	Context("initializing options", func() {
		It("should return an error if not providing a logger", func() {
			option := Options{}
			Expect(option.SetZeroToDefault()).To(HaveOccurred())
		})

		It("should return an error if not providing a self PeerAddress", func() {
			option := Options{
				Logger: logrus.StandardLogger(),
			}
			Expect(option.SetZeroToDefault()).To(HaveOccurred())
		})

		It("should return an error if not providing a PeerAddressCodec", func() {
			option := Options{
				Logger: logrus.StandardLogger(),
				Me:     RandomAddress(),
			}
			Expect(option.SetZeroToDefault()).To(HaveOccurred())
		})

		It("should set unset filed to default value", func() {
			option := Options{
				Logger: logrus.StandardLogger(),
				Me:     RandomAddress(),
				Codec:  SimpleTCPPeerAddressCodec{},
			}
			Expect(option.SetZeroToDefault()).NotTo(HaveOccurred())
			Expect(option.Capacity).Should(Equal(1024))
			Expect(option.NumWorkers).Should(Equal(2 * runtime.NumCPU()))
			Expect(option.Alpha).Should(Equal(24))
			Expect(option.BootstrapDuration).Should(Equal(time.Hour))
		})
	})
})
