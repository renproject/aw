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

var _ = Describe("ECDSA handshake", func() {
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
			defer listener.Close()

			go func() {
				defer GinkgoRecover()

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
			clientHandshaker := handshake.NewECDSA(
				handshake.DefaultOptions().
					WithPrivKey(clientPrivKey).
					WithTimeout(time.Second),
			)
			_, err = clientHandshaker.Handshake(ctx, conn)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("when the client is offline", func() {
		It("should timeout", func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			//
			// Server privkey
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
				serverHandshaker := handshake.NewECDSA(
					handshake.DefaultOptions().
						WithPrivKey(serverPrivKey).
						WithTimeout(time.Second),
				)
				_, err = serverHandshaker.AcceptHandshake(ctx, conn)
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
			// Client privkey and signatory
			//
			clientPrivKey := id.NewPrivKey()
			clientSignatory := id.NewSignatory((*id.PubKey)(&clientPrivKey.PublicKey))

			//
			// Server privkey and signatory
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
				serverHandshaker := handshake.NewECDSA(
					handshake.DefaultOptions().
						WithPrivKey(serverPrivKey).
						WithTimeout(time.Second),
				)
				serverSession, err := serverHandshaker.AcceptHandshake(ctx, conn)
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
			clientHandshaker := handshake.NewECDSA(
				handshake.DefaultOptions().
					WithPrivKey(clientPrivKey).
					WithTimeout(time.Second),
			)
			clientSession, err := clientHandshaker.Handshake(ctx, conn)
			Expect(err).ToNot(HaveOccurred())
			Expect(clientSession.RemoteSignatory()).To(Equal(serverSignatory))
		})

		It("should encrypt/decrypt using plain-text", func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			//
			// Message to be encrypted/decrypted
			//
			plainText := make([]byte, 32)
			for i := range plainText {
				plainText[i] = byte(rand.Int())
			}

			//
			// Client privkey and signatory
			//
			clientPrivKey := id.NewPrivKey()
			clientSignatory := id.NewSignatory((*id.PubKey)(&clientPrivKey.PublicKey))

			//
			// Server privkey and signatory
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
				serverHandshaker := handshake.NewECDSA(
					handshake.DefaultOptions().
						WithPrivKey(serverPrivKey).
						WithTimeout(time.Second),
				)
				serverSession, err := serverHandshaker.AcceptHandshake(ctx, conn)
				Expect(err).ToNot(HaveOccurred())
				Expect(serverSession.RemoteSignatory()).To(Equal(clientSignatory))

				//
				// Server encryption/decryption
				//
				cipherText, err := serverSession.Encrypt(plainText[:])
				Expect(err).ToNot(HaveOccurred())
				Expect(bytes.Equal(plainText, cipherText)).To(BeFalse())

				n, err := conn.Write(cipherText)
				Expect(n).To(Equal(len(cipherText)))
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
			clientHandshaker := handshake.NewECDSA(
				handshake.DefaultOptions().
					WithPrivKey(clientPrivKey).
					WithTimeout(time.Second),
			)
			clientSession, err := clientHandshaker.Handshake(ctx, conn)
			Expect(err).ToNot(HaveOccurred())
			Expect(clientSession.RemoteSignatory()).To(Equal(serverSignatory))

			//
			// Client encryption/decryption
			//
			cipherText := [1024]byte{}
			n, err := conn.Read(cipherText[:])
			Expect(err).ToNot(HaveOccurred())

			decryptedCipherText, err := clientSession.Decrypt(cipherText[:n])
			Expect(err).ToNot(HaveOccurred())
			Expect(bytes.Equal(plainText, decryptedCipherText)).To(BeTrue())
		})
	})

	// TODO:
	// 1. Test filtering.
	// 2. Test client sending malformed data.
	// 3. Test server sending malformed data.
	// 4. Refactor tests to reduce duplication.
})
