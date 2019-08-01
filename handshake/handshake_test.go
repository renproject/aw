package handshake_test

import (
	"context"
	"net"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/handshake"
	"github.com/renproject/phi"

	"github.com/renproject/aw/testutil"
)

var _ = Describe("Handshaker", func() {
	Context("when both parties are authenticated", func() {
		It("should sucessfully perform the handshake", func() {
			clientSV := testutil.NewMockSignVerifier()
			serverSV := testutil.NewMockSignVerifier(clientSV.ID())
			clientSV.Whitelist(serverSV.ID())
			client := New(clientSV)
			server := New(serverSV)

			c2s, s2c := net.Pipe()
			phi.ParBegin(
				func() {
					defer GinkgoRecover()
					Expect(client.Handshake(context.Background(), c2s)).Should(BeNil())
				},
				func() {
					defer GinkgoRecover()
					Expect(server.AcceptHandshake(context.Background(), s2c)).Should(BeNil())
				},
			)
		})
	})
})
