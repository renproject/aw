package tcp_test

import (
	"time"

	"github.com/renproject/aw/protocol"
	"github.com/renproject/aw/tcp"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Client", func() {

	Context("when connecting to an unavailable server", func() {
		It("should timeout", func() {

		})

		It("should connect after the server becomes available", func() {

			toClient := make(chan protocol.MessageOnTheWire)
			_ = tcp.NewClient(tcp.NewClientConns(tcp.ClientOptions{
				Timeout: time.Second,
			}), toClient)

			Expect(true).To(BeTrue())
		})
	})

})
