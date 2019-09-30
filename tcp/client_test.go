package tcp_test

import (
	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/handshake/session"
	"github.com/renproject/aw/protocol"
	"github.com/renproject/aw/tcp"
	"github.com/renproject/aw/testutil"
	"github.com/renproject/kv"
	"github.com/sirupsen/logrus"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Client", func() {

	Context("when connecting to an unavailable server", func() {
		It("should timeout", func() {

		})

		It("should connect after the server becomes available", func() {
			addr := testutil.NewSimpleTCPPeerAddress("peer-1", "127.0.0.1", "5000")
			codec := testutil.NewSimpleTCPPeerAddressCodec()

			dht, err := dht.New(addr, codec, kv.NewTable(kv.NewMemDB(kv.JSONCodec), "dht"))
			Expect(err).ShouldNot(HaveOccurred())

			toClient := make(chan protocol.MessageOnTheWire)
			hs := handshake.New(testutil.NewMockSignVerifier(), session.NewNOPSessionCreator())
			_ = tcp.NewClient(tcp.ClientOptions{
				Logger:     logrus.StandardLogger(),
				Handshaker: hs,
			}, tcp.NewConnPool(tcp.ConnPoolOptions{}), dht, toClient)
		})
	})

})
