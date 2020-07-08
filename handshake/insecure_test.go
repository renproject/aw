package handshake_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/renproject/aw/handshake"
	"github.com/renproject/id"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Insecure handshake", func() {
	Context("when the server is offline", func() {
		It("should timeout", func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			//
			// Client privkey
			//
			clientPrivKey := id.NewPrivKey()

			//
			// Server connection
			//
			port := uint16(3000 + rand.Int()%3000)
			listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%v", port))
			Expect(err).ToNot(HaveOccurred())

			go func() {
				defer GinkgoRecover()
				defer listener.Close()

				_, err := listener.Accept()
				Expect(err).ToNot(HaveOccurred())
			}()

			//
			// Client connection
			//
			conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%v", port))
			Expect(err).ToNot(HaveOccurred())

			//
			// Client handshake
			//
			clientHandshake := handshake.NewInsecure(
				handshake.DefaultOptions().
					WithPrivKey(clientPrivKey).
					WithTimeout(time.Second),
			)
			_, err = clientHandshake.Handshake(ctx, conn)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("when the client is offline", func() {
		It("should timeout", func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			//
			// Server signatory
			//
			serverPrivKey := id.NewPrivKey()

			//
			// Server connection
			//
			port := uint16(3000 + rand.Int()%3000)
			listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%v", port))
			Expect(err).ToNot(HaveOccurred())

			go func() {
				defer GinkgoRecover()
				defer listener.Close()

				conn, err := listener.Accept()
				Expect(err).ToNot(HaveOccurred())

				//
				// Server handshake
				//
				serverHandshake := handshake.NewInsecure(
					handshake.DefaultOptions().
						WithPrivKey(serverPrivKey).
						WithTimeout(time.Second),
				)
				_, err = serverHandshake.AcceptHandshake(ctx, conn)
				Expect(err).To(HaveOccurred())
			}()

			//
			// Client connection
			//
			_, err = net.Dial("tcp", fmt.Sprintf("127.0.0.1:%v", port))
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("when the client and server are online", func() {
		It("should exchange signatories", func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			//
			// Client signatory
			//
			clientPrivKey := id.NewPrivKey()
			clientSignatory := id.NewSignatory((*id.PubKey)(&clientPrivKey.PublicKey))

			//
			// Server signatory
			//
			serverPrivKey := id.NewPrivKey()
			serverSignatory := id.NewSignatory((*id.PubKey)(&serverPrivKey.PublicKey))

			//
			// Server connection
			//
			port := uint16(3000 + rand.Int()%3000)
			listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%v", port))
			Expect(err).ToNot(HaveOccurred())

			go func() {
				defer GinkgoRecover()
				defer listener.Close()

				conn, err := listener.Accept()
				Expect(err).ToNot(HaveOccurred())

				//
				// Server handshake
				//
				serverHandshake := handshake.NewInsecure(
					handshake.DefaultOptions().
						WithPrivKey(serverPrivKey).
						WithTimeout(time.Second),
				)
				serverSession, err := serverHandshake.AcceptHandshake(ctx, conn)
				Expect(err).ToNot(HaveOccurred())
				Expect(serverSession.RemoteSignatory()).To(Equal(clientSignatory))
			}()

			//
			// Client connection
			//
			conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%v", port))
			Expect(err).ToNot(HaveOccurred())

			//
			// Client handshake
			//
			clientHandshake := handshake.NewInsecure(
				handshake.DefaultOptions().
					WithPrivKey(clientPrivKey).
					WithTimeout(time.Second),
			)
			clientSession, err := clientHandshake.Handshake(ctx, conn)
			Expect(err).ToNot(HaveOccurred())
			Expect(clientSession.RemoteSignatory()).To(Equal(serverSignatory))
		})

		It("should encrypt/decrypt using plain-text", func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			//
			// Client signatory
			//
			clientPrivKey := id.NewPrivKey()
			clientSignatory := id.NewSignatory((*id.PubKey)(&clientPrivKey.PublicKey))

			//
			// Server signatory
			//
			serverPrivKey := id.NewPrivKey()
			serverSignatory := id.NewSignatory((*id.PubKey)(&serverPrivKey.PublicKey))

			//
			// Server connection
			//
			port := uint16(3000 + rand.Int()%3000)
			listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%v", port))
			Expect(err).ToNot(HaveOccurred())

			go func() {
				defer GinkgoRecover()
				defer listener.Close()

				conn, err := listener.Accept()
				Expect(err).ToNot(HaveOccurred())

				//
				// Server handshake
				//
				serverHandshake := handshake.NewInsecure(
					handshake.DefaultOptions().
						WithPrivKey(serverPrivKey).
						WithTimeout(time.Second),
				)
				serverSession, err := serverHandshake.AcceptHandshake(ctx, conn)
				Expect(err).ToNot(HaveOccurred())
				Expect(serverSession.RemoteSignatory()).To(Equal(clientSignatory))

				//
				// Server encryption/decryption
				//
				plainText := make([]byte, 32)
				for i := range plainText {
					plainText[i] = byte(rand.Int())
				}
				cipherText, err := serverSession.Encrypt(plainText)
				Expect(err).ToNot(HaveOccurred())
				Expect(bytes.Equal(plainText, cipherText)).To(BeTrue())

				plainText, err = serverSession.Decrypt(cipherText)
				Expect(err).ToNot(HaveOccurred())
				Expect(bytes.Equal(plainText, cipherText)).To(BeTrue())
			}()

			//
			// Client connection
			//
			conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%v", port))
			Expect(err).ToNot(HaveOccurred())

			//
			// Client handshake
			//
			clientHandshake := handshake.NewInsecure(
				handshake.DefaultOptions().
					WithPrivKey(clientPrivKey).
					WithTimeout(time.Second),
			)
			clientSession, err := clientHandshake.Handshake(ctx, conn)
			Expect(err).ToNot(HaveOccurred())
			Expect(clientSession.RemoteSignatory()).To(Equal(serverSignatory))

			//
			// Client encryption/decryption
			//
			plainText := make([]byte, 32)
			for i := range plainText {
				plainText[i] = byte(rand.Int())
			}
			cipherText, err := clientSession.Encrypt(plainText)
			Expect(err).ToNot(HaveOccurred())
			Expect(bytes.Equal(plainText, cipherText)).To(BeTrue())

			plainText, err = clientSession.Decrypt(cipherText)
			Expect(err).ToNot(HaveOccurred())
			Expect(bytes.Equal(plainText, cipherText)).To(BeTrue())
		})
	})
})
