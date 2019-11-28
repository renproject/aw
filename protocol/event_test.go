package protocol_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/protocol"
)

var _ = Describe("Events", func() {
	Context("when defining EventPeerChanged", func() {
		It("should implement the Event interface", func() {
			Expect(func() { EventPeerChanged{}.IsEvent() }).ToNot(Panic())
		})
	})

	Context("when defining EventMessageReceived", func() {
		It("should implement the Event interface", func() {
			Expect(func() { EventMessageReceived{}.IsEvent() }).ToNot(Panic())
		})
	})
})
