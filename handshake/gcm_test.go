package handshake_test

import (
	"bytes"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/handshake"
	. "github.com/renproject/aw/testutil"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/renproject/aw/protocol"
)

var _ = Describe("GCM session manager", func() {
	Context("when using a GCM session managers", func() {
		Context("when generating a session key", func() {
			It("should generate a random 32 byte session key", func() {
				keys := map[string]struct{}{}

				test := func() bool {
					manager := NewGCMSessionManager()
					key := manager.NewSessionKey()
					_, ok := keys[string(key)]
					return !ok
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when writing and reading message with the same session keys", func() {
			It("should be able to write and then read", func() {
				test := func() bool {
					manager := NewGCMSessionManager()
					sender, receiver := RandomPeerID(), RandomPeerID()
					key := manager.NewSessionKey()
					senderSession := manager.NewSession(receiver, key)
					receiverSession := manager.NewSession(sender, key)

					buf := bytes.NewBuffer([]byte{})
					sentMsg := RandomMessage(protocol.V1, RandomMessageVariant())
					Expect(senderSession.WriteMessage(buf, sentMsg)).NotTo(HaveOccurred())

					receivedMsg, err := receiverSession.ReadMessageOnTheWire(buf)
					Expect(err).NotTo(HaveOccurred())
					Expect(receivedMsg.From.Equal(sender)).Should(BeTrue())
					return cmp.Equal(receivedMsg.Message, sentMsg, cmpopts.EquateEmpty())
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when writing and reading message with the different session keys", func() {
			It("should not be able to write and then read", func() {
				test := func() bool {
					manager := NewGCMSessionManager()
					sender := RandomPeerID()
					session1 := manager.NewSession(sender, manager.NewSessionKey())
					session2 := manager.NewSession(sender, manager.NewSessionKey())

					buf := bytes.NewBuffer([]byte{})
					sentMsg := RandomMessage(protocol.V1, RandomMessageVariant())
					Expect(session1.WriteMessage(buf, sentMsg)).NotTo(HaveOccurred())

					_, err := session2.ReadMessageOnTheWire(buf)
					Expect(err).To(HaveOccurred())
					return true
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})
	})
})
