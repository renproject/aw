package protocol_test

import (
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/protocol"
	. "github.com/renproject/aw/testutil"
)

var _ = Describe("PeerAddress", func() {
	Context("PeerGroupID", func() {
		Context("when validating message GroupID", func() {
			It("should be nil for Ping, Pong and Cast message", func() {
				Expect(ValidatePeerGroupID(NilPeerGroupID, Ping)).To(BeNil())
				Expect(ValidatePeerGroupID(NilPeerGroupID, Pong)).To(BeNil())
				Expect(ValidatePeerGroupID(NilPeerGroupID, Cast)).To(BeNil())
				Expect(ValidatePeerGroupID(NilPeerGroupID, Multicast)).To(BeNil())
				Expect(ValidatePeerGroupID(NilPeerGroupID, Broadcast)).To(BeNil())

				Expect(ValidatePeerGroupID(RandomPeerGroupID(), Ping)).NotTo(BeNil())
				Expect(ValidatePeerGroupID(RandomPeerGroupID(), Pong)).NotTo(BeNil())
				Expect(ValidatePeerGroupID(RandomPeerGroupID(), Cast)).NotTo(BeNil())
				Expect(ValidatePeerGroupID(RandomPeerGroupID(), Multicast)).To(BeNil())
				Expect(ValidatePeerGroupID(RandomPeerGroupID(), Broadcast)).To(BeNil())
			})
		})

		Context("when comparing groupID", func() {
			It("should tell whether two groupID are the same", func() {
				test := func() bool {
					id := RandomPeerGroupID()
					return id.Equal(id)
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})
	})
})
