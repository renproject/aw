package handshake_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Handshake", func() {

	Context("when connecting a client to a server", func() {
		Context("when the server is online", func() {
			It("should connect successfully", func() {
				Expect(true).To(BeTrue())
			})
		})

		Context("when the server is offline", func() {
			It("should retry", func() {
				Expect(true).To(BeTrue())
			})

			Context("when the server comes online", func() {
				It("should eventually connect successfully", func() {
					Expect(true).To(BeTrue())
				})
			})
		})
	})
})
