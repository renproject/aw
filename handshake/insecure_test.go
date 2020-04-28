package handshake_test

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
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

			port := uint16(3000 + rand.Int()%3000)
			listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%v", port))
			Expect(err).ToNot(HaveOccurred())
			defer listener.Close()

			go func() {
				_, err := listener.Accept()
				Expect(err).ToNot(HaveOccurred())
			}()

			conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%v", port))
			Expect(err).ToNot(HaveOccurred())

			clientPrivKey, err := crypto.GenerateKey()
			Expect(err).ToNot(HaveOccurred())
			clientSignatory := id.NewSignatory(&clientPrivKey.PublicKey)
			clientHandshake := handshake.NewInsecure(clientSignatory, time.Second)
			_, err = clientHandshake.Handshake(ctx, conn)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("when the client is offline", func() {
		It("should timeout", func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			port := uint16(3000 + rand.Int()%3000)
			listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%v", port))
			Expect(err).ToNot(HaveOccurred())
			defer listener.Close()

			go func() {
				conn, err := listener.Accept()
				Expect(err).ToNot(HaveOccurred())

				serverPrivKey, err := crypto.GenerateKey()
				Expect(err).ToNot(HaveOccurred())
				serverSignatory := id.NewSignatory(&serverPrivKey.PublicKey)
				serverHandshake := handshake.NewInsecure(serverSignatory, time.Second)
				_, err = serverHandshake.Handshake(ctx, conn)
				Expect(err).To(HaveOccurred())
			}()

			_, err = net.Dial("tcp", fmt.Sprintf("127.0.0.1:%v", port))
			Expect(err).ToNot(HaveOccurred())
		})
	})

	Context("when the client and server are online", func() {
		It("should exchange signatories", func() {
			Expect(func() { panic("unimplemented") }).ToNot(Panic())
		})
	})
})
