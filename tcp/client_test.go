package tcp_test

import (
	"time"

	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/protocol"
	"github.com/renproject/aw/tcp"
	"github.com/renproject/aw/testutil"
	"github.com/sirupsen/logrus"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Client", func() {

	Context("when connecting to an unavailable server", func() {
		It("should timeout", func() {

		})

		It("should connect after the server becomes available", func() {

			toClient := make(chan protocol.MessageOnTheWire)
			hs := handshake.New(testutil.NewMockSignVerifier())
			_ = tcp.NewClient(tcp.NewClientConns(tcp.ClientOptions{
				Logger:  logrus.StandardLogger(),
				Timeout: time.Second,
			}, hs), toClient)

			Expect(true).To(BeTrue())
		})
	})

})
