package handshake_test

import (
	"context"
	"net"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/handshake"

	"github.com/renproject/aw/handshake/session"
	"github.com/renproject/aw/testutil"
	"github.com/renproject/phi"
)

var _ = Describe("Handshaker", func() {
	Context("when both parties are authenticated", func() {
		It("should sucessfully perform the handshake with gcm session creator", func() {
			clientSV := testutil.NewMockSignVerifier()
			serverSV := testutil.NewMockSignVerifier(clientSV.ID())
			clientSV.Whitelist(serverSV.ID())

			sessionCreator := session.NewGCMSessionCreator()
			client := New(clientSV, sessionCreator)
			server := New(serverSV, sessionCreator)

			c2s, s2c := net.Pipe()
			phi.ParBegin(
				func() {
					defer GinkgoRecover()
					_, err := client.Handshake(context.Background(), c2s)
					Expect(err).Should(BeNil())
				},
				func() {
					defer GinkgoRecover()
					_, err := server.AcceptHandshake(context.Background(), s2c)
					Expect(err).Should(BeNil())
				},
			)
		})
	})

	It("should sucessfully perform the handshake with nop session creator", func() {
		clientSV := testutil.NewMockSignVerifier()
		serverSV := testutil.NewMockSignVerifier(clientSV.ID())
		clientSV.Whitelist(serverSV.ID())

		sessionCreator := session.NewNOPSessionCreator()
		client := New(clientSV, sessionCreator)
		server := New(serverSV, sessionCreator)

		c2s, s2c := net.Pipe()
		phi.ParBegin(
			func() {
				defer GinkgoRecover()
				_, err := client.Handshake(context.Background(), c2s)
				Expect(err).Should(BeNil())
			},
			func() {
				defer GinkgoRecover()
				_, err := server.AcceptHandshake(context.Background(), s2c)
				Expect(err).Should(BeNil())
			},
		)
	})
})
