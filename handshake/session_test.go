package handshake_test

import (
	"bytes"
	"crypto/rand"
	"testing/quick"

	"github.com/renproject/aw/protocol"
	"github.com/renproject/aw/testutil"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/handshake"
)

var _ = Describe("Session", func() {
	Context("when both parties are honest", func() {
		It("should successfully cipher and decipher", func() {
			secret := [32]byte{}
			rand.Read(secret[:])
			client := testutil.SimplePeerID("client")
			server := testutil.SimplePeerID("server")
			// clientConn, serverConn := net.Pipe()
			buffer := new(bytes.Buffer)

			clientSession := NewGCMSession(server, secret)
			serverSession := NewGCMSession(client, secret)

			check := func(text [32]byte) bool {
				msg := protocol.NewMessage(protocol.V1, protocol.Cast, text[:])
				clientSession.WriteMessage(msg, buffer)
				motw, err := serverSession.ReadMessage(buffer)
				if err != nil {
					return false
				}
				rcvMsg := [32]byte{}
				copy(rcvMsg[:], motw.Message.Body)
				return motw.From.Equal(client) && rcvMsg == text
			}
			Expect(quick.Check(check, nil)).Should(BeNil())
		})

		It("should successfully cipher and decipher", func() {
			secret := [32]byte{}
			rand.Read(secret[:])
			client := testutil.SimplePeerID("client")
			server := testutil.SimplePeerID("server")
			// clientConn, serverConn := net.Pipe()
			buffer := new(bytes.Buffer)

			clientSession := NewNOPSession(server)
			serverSession := NewNOPSession(client)

			check := func(text [32]byte) bool {
				msg := protocol.NewMessage(protocol.V1, protocol.Cast, text[:])
				clientSession.WriteMessage(msg, buffer)
				motw, err := serverSession.ReadMessage(buffer)
				if err != nil {
					return false
				}
				rcvMsg := [32]byte{}
				copy(rcvMsg[:], motw.Message.Body)
				return motw.From.Equal(client) && rcvMsg == text
			}
			Expect(quick.Check(check, nil)).Should(BeNil())
		})
	})
})
