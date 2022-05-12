package handshake_test

import (
	"context"
	"fmt"
	"net"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/session"
	"github.com/renproject/id"
)

var _ = Describe("handshake", func() {
	It("should handshake successfully under normal conditions", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		listenerPrivKey := id.NewPrivKey()
		listenerSignatory := listenerPrivKey.Signatory()
		dialerPrivKey := id.NewPrivKey()
		dialerSignatory := dialerPrivKey.Signatory()

		listener, err := new(net.ListenConfig).Listen(ctx, "tcp", "localhost:0")
		if err != nil {
			panic(err)
		}
		port := uint16(listener.Addr().(*net.TCPAddr).Port)

		sessionCh := make(chan *session.GCMSession, 1)
		go func() {
			conn, err := listener.Accept()
			if err != nil {
				panic(err)
			}

			session, remote, err := handshake.Handshake(listenerPrivKey, conn)
			Expect(err).ToNot(HaveOccurred())
			Expect(remote.Equal(&dialerSignatory)).To(BeTrue())

			sessionCh <- session
		}()

		dialer := new(net.Dialer)

		var dialerSession *session.GCMSession
		{
			conn, err := dialer.Dial("tcp", fmt.Sprintf("localhost:%v", port))
			if err != nil {
				panic(err)
			}

			session, remote, err := handshake.Handshake(dialerPrivKey, conn)
			Expect(err).ToNot(HaveOccurred())
			Expect(remote.Equal(&listenerSignatory)).To(BeTrue())

			dialerSession = session
		}

		listenerSession := <-sessionCh

		encodeBuf := make([]byte, 1024)
		decodeBuf := make([]byte, 1024)
		listenerMsg := []byte("message from listener")
		dialerMsg := []byte("message from dialer")

		// Listener can decode encoded message from dialer
		dialerWriteNonceBuf := dialerSession.GetWriteNonceAndIncrement()
		sealed := dialerSession.GCM.Seal(encodeBuf[:0], dialerWriteNonceBuf[:], dialerMsg, nil)

		listenerReadNonceBuf := listenerSession.GetReadNonceAndIncrement()
		decrypted, err := listenerSession.GCM.Open(decodeBuf[:0], listenerReadNonceBuf[:], sealed, nil)
		Expect(err).ToNot(HaveOccurred())
		Expect(decrypted).To(Equal(dialerMsg))

		// Dialer can decode encoded message from Listener
		listenerWriteNonceBuf := listenerSession.GetWriteNonceAndIncrement()
		sealed = listenerSession.GCM.Seal(encodeBuf[:0], listenerWriteNonceBuf[:], listenerMsg, nil)

		dialerReadNonceBuf := dialerSession.GetReadNonceAndIncrement()
		decrypted, err = dialerSession.GCM.Open(decodeBuf[:0], dialerReadNonceBuf[:], sealed, nil)
		Expect(err).ToNot(HaveOccurred())
		Expect(decrypted).To(Equal(listenerMsg))
	})
})
