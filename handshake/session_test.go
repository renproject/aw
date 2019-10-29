package handshake_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Session", func() {
	Context("when using an insecure session managers", func() {
		Context("when generating a session key", func() {
			It("should generate an empty session key", func() {
				Expect(true).To(BeFalse())
			})
		})

		Context("when writing and reading message", func() {
			It("should be able to write and then read", func() {
				Expect(true).To(BeFalse())
			})
		})
	})

	Context("when using a GCM session managers", func() {
		Context("when generating a session key", func() {
			It("should generate a random 32 byte session key", func() {
				Expect(true).To(BeFalse())
			})
		})

		Context("when writing and reading message with the same session keys", func() {
			It("should be able to write and then read", func() {
				Expect(true).To(BeFalse())
			})
		})

		Context("when writing and reading message with the different session keys", func() {
			It("should not be able to write and then read", func() {
				Expect(true).To(BeFalse())
			})
		})
	})
})
