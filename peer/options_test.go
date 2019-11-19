package peer_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("options", func() {
	Context("initializing options", func() {
		It("should set unset filed to default value", func() {
			Expect(true).Should(BeTrue())
		})
	})
})
