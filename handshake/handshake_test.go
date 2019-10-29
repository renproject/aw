package handshake_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Handshaker", func() {
	Context("when using gcm handshakes", func() {
		Context("when the client is honest", func() {
			Context("when the server is honest", func() {
				Context("when handshaking", func() {
					It("should authenticate the client and server", func() {
						Expect(true).To(BeFalse())
					})

					It("should cipher and decipher messages", func() {
						Expect(true).To(BeFalse())
					})
				})
			})

			Context("when the server is dishonest", func() {
				Context("when the server sends a malformed rsa.PublicKey", func() {
					It("should return an error", func() {
						Expect(true).To(BeFalse())
					})
				})

				Context("when the server sends an incorrectly encrypted session key", func() {
					It("should return an error", func() {
						Expect(true).To(BeFalse())
					})
				})

				Context("when the server sends an invalid session key", func() {
					It("should return an error", func() {
						Expect(true).To(BeFalse())
					})
				})

				Context("when the server sends an invalid signature for the server session key", func() {
					It("should return an error", func() {
						Expect(true).To(BeFalse())
					})
				})

				Context("when the server sends an invalid signature for the server rsa.PublicKey", func() {
					It("should return an error", func() {
						Expect(true).To(BeFalse())
					})
				})

				Context("when the server sends a valid signaturefor the server rsa.PublicKey that does not match the signature for the server session key", func() {
					It("should return an error", func() {
						Expect(true).To(BeFalse())
					})
				})

				Context("when the server sends an invalid signature for the client session key", func() {
					It("should return an error", func() {
						Expect(true).To(BeFalse())
					})
				})

				Context("when the server sends a valid signature for the client session key that does not match the signature for the server session key", func() {
					It("should return an error", func() {
						Expect(true).To(BeFalse())
					})
				})
			})
		})

		Context("when the server is honest", func() {
			Context("when the client is dishonest", func() {
				Context("when the client sends a malformed rsa.PublicKey", func() {
					It("should return an error", func() {
						Expect(true).To(BeFalse())
					})
				})

				Context("when the client sends an invalid signature for their rsa.PublicKey", func() {
					It("should return an error", func() {
						Expect(true).To(BeFalse())
					})
				})

				Context("when the client sends an invalid signature for the server session key", func() {
					It("should return an error", func() {
						Expect(true).To(BeFalse())
					})
				})

				Context("when the client sends a valid signature for the server session key that does not match the signature for the client rsa.PublicKey", func() {
					It("should return an error", func() {
						Expect(true).To(BeFalse())
					})
				})

				Context("when the client sends an incorrectly encrypted session key", func() {
					It("should return an error", func() {
						Expect(true).To(BeFalse())
					})
				})

				Context("when the client sends an invalid signature for the client session key", func() {
					It("should return an error", func() {
						Expect(true).To(BeFalse())
					})
				})

				Context("when the client sends a valid signature for the client session key that does not match the signature for the client rsa.PublicKey", func() {
					It("should return an error", func() {
						Expect(true).To(BeFalse())
					})
				})
			})
		})
	})
})
