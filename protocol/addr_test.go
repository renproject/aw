package protocol_test

import (
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/protocol"
	. "github.com/renproject/aw/testutil"
)

var _ = Describe("PeerAddress", func() {
	Context("GroupID", func() {
		Context("when validating message GroupID", func() {
			It("should be nil for Ping, Pong and Cast message", func() {
				Expect(ValidateGroupID(NilGroupID, Ping)).To(BeNil())
				Expect(ValidateGroupID(NilGroupID, Pong)).To(BeNil())
				Expect(ValidateGroupID(NilGroupID, Cast)).To(BeNil())
				Expect(ValidateGroupID(NilGroupID, Multicast)).To(BeNil())
				Expect(ValidateGroupID(NilGroupID, Broadcast)).To(BeNil())

				Expect(ValidateGroupID(RandomGroupID(), Ping)).NotTo(BeNil())
				Expect(ValidateGroupID(RandomGroupID(), Pong)).NotTo(BeNil())
				Expect(ValidateGroupID(RandomGroupID(), Cast)).NotTo(BeNil())
				Expect(ValidateGroupID(RandomGroupID(), Multicast)).To(BeNil())
				Expect(ValidateGroupID(RandomGroupID(), Broadcast)).To(BeNil())
			})
		})

		Context("when comparing groupID", func() {
			It("should tell whether two groupID are the same", func() {
				test := func() bool {
					id := RandomGroupID()
					return id.Equal(id)
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})
	})
})
