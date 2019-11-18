package handshake_test

import (
	"bytes"
	"reflect"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/handshake"
	. "github.com/renproject/aw/testutil"

	"github.com/renproject/aw/protocol"
)

var _ = Describe("Insecure session manager", func() {
	Context("when using an insecure session managers", func() {
		Context("when generating a session key", func() {
			It("should generate an empty session key", func() {
				test := func() bool {
					manager := NewInsecureSessionManager()
					key := manager.NewSessionKey()
					return bytes.Equal(key, []byte{})
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when writing and reading message", func() {
			It("should be able to write and then read", func() {
				test := func() bool {
					manager := NewInsecureSessionManager()
					sender := RandomPeerID()
					session := manager.NewSession(sender, nil)

					buf := bytes.NewBuffer([]byte{})
					sentMsg := RandomMessage(protocol.V1, RandomMessageVariant())
					Expect(session.WriteMessage(buf, sentMsg)).NotTo(HaveOccurred())

					receivedMsg, err := session.ReadMessageOnTheWire(buf)
					Expect(err).NotTo(HaveOccurred())
					Expect(receivedMsg.From.Equal(sender)).Should(BeTrue())
					return reflect.DeepEqual(receivedMsg.Message, sentMsg)
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})
	})
})
