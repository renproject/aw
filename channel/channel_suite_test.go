package channel_test

import (
	"context"
	"fmt"
	"log"
	"net"
	"testing"
	"time"

	"github.com/renproject/aw/channel"
	"github.com/renproject/aw/codec"
	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/policy"
	"github.com/renproject/aw/tcp"
	"github.com/renproject/id"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestChannel(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Channel suite")
}

func listen(ctx context.Context, attacher channel.Attacher, self, other id.Signatory, port uint16) {
	go func() {
		defer GinkgoRecover()
		Expect(tcp.Listen(
			ctx,
			fmt.Sprintf("127.0.0.1:%v", port),
			func(conn net.Conn) {
				log.Printf("accepted: %v", conn.RemoteAddr())
				enc, dec, remote, err := handshake.Insecure(self)(
					conn,
					codec.LengthPrefixEncoder(codec.PlainEncoder),
					codec.LengthPrefixDecoder(codec.PlainDecoder),
				)
				if err != nil {
					log.Printf("handshake: %v", err)
					return
				}
				if !other.Equal(&remote) {
					log.Printf("handshake: expected %v, got %v", other, remote)
					return
				}
				if err := attacher.Attach(ctx, remote, conn, enc, dec); err != nil {
					log.Printf("attach listener: %v", err)
					return
				}
			},
			func(err error) {
				log.Printf("listen: %v", err)
			},
			nil,
		)).To(Equal(context.Canceled))
	}()
}

func dial(ctx context.Context, attacher channel.Attacher, self, other id.Signatory, port uint64, retry time.Duration) {
	go func() {
		defer GinkgoRecover()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			// Dial a connection in the background. We do it in the
			// background so that we can dial new connections later (to
			// replace this one, and verify that channels behave as
			// expected under these conditions).
			go func() {
				defer GinkgoRecover()
				Expect(tcp.Dial(
					ctx,
					fmt.Sprintf("127.0.0.1:%v", port),
					func(conn net.Conn) {
						log.Printf("dialed: %v", conn.RemoteAddr())
						enc, dec, remote, err := handshake.Insecure(self)(
							conn,
							codec.LengthPrefixEncoder(codec.PlainEncoder),
							codec.LengthPrefixDecoder(codec.PlainDecoder),
						)
						if err != nil {
							log.Printf("handshake: %v", err)
							return
						}
						if !other.Equal(&remote) {
							log.Printf("handshake: expected %v, got %v", other, remote)
							return
						}
						if err := attacher.Attach(ctx, remote, conn, enc, dec); err != nil {
							log.Printf("attach dialer: %v", err)
							return
						}
					},
					func(err error) {
						log.Printf("dial: %v", err)
					},
					policy.ConstantTimeout(100*time.Millisecond),
				)).To(Succeed())
			}()
			// After some duration, dial again. This will create an
			// entirely new connection, and replace the previous
			// connection.
			<-time.After(retry)
		}
	}()
}
