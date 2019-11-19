package peer_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/peer"
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

		It("should set unset filed to default value", func() {
			option := Options{
				Logger: logrus.StandardLogger(),
				Me:     random,
			}
			Expect(option.SetZeroToDefault()).To(HaveOccurred())
		})
	})
})
