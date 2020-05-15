package handshake_test

import (
	"bytes"
	"math/rand"

	"github.com/renproject/aw/handshake"
	"github.com/renproject/id"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("GCM session", func() {
	Context("when creating a session", func() {
		Context("when the key length is not 16, 24, or 32", func() {
			It("should return an error", func() {
				//
				// Key
				//
				keyLen := 8*(2+rand.Int()%3) + (rand.Int() % 7)
				key := make([]byte, keyLen)
				for i := range key {
					key[i] = byte(rand.Int())
				}
				//
				// Session
				//
				_, err := handshake.NewGCMSession(key, id.Signatory{})
				Expect(err).To(HaveOccurred())
			})
		})

		Context("when the key length is 16, 24, or 32", func() {
			It("should not return an error", func() {
				//
				// Key
				//
				keyLen := 8 * (2 + rand.Int()%3)
				key := make([]byte, keyLen)
				for i := range key {
					key[i] = byte(rand.Int())
				}
				//
				// Session
				//
				_, err := handshake.NewGCMSession(key, id.Signatory{})
				Expect(err).ToNot(HaveOccurred())
			})

			It("should return the remote signatory", func() {
				//
				// Key
				//
				keyLen := 8 * (2 + rand.Int()%3)
				key := make([]byte, keyLen)
				for i := range key {
					key[i] = byte(rand.Int())
				}
				//
				// Signatory
				//
				signatory := id.Signatory{}
				for i := range signatory {
					signatory[i] = byte(rand.Int())
				}
				//
				// Session
				//
				session, err := handshake.NewGCMSession(key, signatory)
				Expect(err).ToNot(HaveOccurred())
				Expect(session.RemoteSignatory()).To(Equal(signatory))
			})
		})
	})

	Context("when encrypting", func() {
		It("should not return the plain-text", func() {
			//
			// Session
			//
			keyLen := 8 * (2 + rand.Int()%3)
			key := make([]byte, keyLen)
			for i := range key {
				key[i] = byte(rand.Int())
			}
			session, err := handshake.NewGCMSession(key, id.Signatory{})
			Expect(err).ToNot(HaveOccurred())
			//
			// Encrypt
			//
			plainText := make([]byte, 32)
			for i := range plainText {
				plainText[i] = byte(rand.Int())
			}
			cipherText, err := session.Encrypt(plainText[:])
			Expect(err).ToNot(HaveOccurred())
			Expect(bytes.Equal(plainText, cipherText)).To(BeFalse())
		})

		Context("when decrypting", func() {
			It("should return the plain-text", func() {
				//
				// Session
				//
				keyLen := 8 * (2 + rand.Int()%3)
				key := make([]byte, keyLen)
				for i := range key {
					key[i] = byte(rand.Int())
				}
				session1, err := handshake.NewGCMSession(key, id.Signatory{})
				Expect(err).ToNot(HaveOccurred())
				session2, err := handshake.NewGCMSession(key, id.Signatory{})
				Expect(err).ToNot(HaveOccurred())
				//
				// Encrypt
				//
				plainText := make([]byte, 32)
				for i := range plainText {
					plainText[i] = byte(rand.Int())
				}
				cipherText, err := session1.Encrypt(plainText)
				Expect(err).ToNot(HaveOccurred())
				Expect(bytes.Equal(plainText, cipherText)).To(BeFalse())
				//
				// Decrypt
				//
				decryptedCipherText, err := session2.Decrypt(cipherText)
				Expect(err).ToNot(HaveOccurred())
				Expect(bytes.Equal(plainText, decryptedCipherText)).To(BeTrue())
			})
		})
	})
})
