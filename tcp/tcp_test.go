package tcp_test

import (
	"context"
	"io"
	"math/rand"
	"net"
	"time"

	"github.com/renproject/aw/experiment/policy"
	"github.com/renproject/aw/experiment/tcp"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("TCP", func() {

	// run test scenarios with some configurations that enable enough variation
	// to test everything we are interested in testing.
	run := func(ctx context.Context, dialDelay, listenerDelay time.Duration, enableListener, rejectInboundConns bool) {
		// Channel for communicating to the main goroutine which messages have
		// been received by the listener and the dialer.
		messageReceived := make(chan [100]byte, 2)
		message := [100]byte{}

		// Generate a random message.
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		r.Read(message[:])

		if enableListener {
			allow := policy.Max(0)
			if !rejectInboundConns {
				// We want to allow the dialing attempt. To catch different
				// cases, sometimes we use an explicit policy, and other times
				// we use a nil policy.
				if r.Int()%2 == 0 {
					allow = policy.Max(1)
				} else {
					allow = nil
				}
			}
			go func() {
				defer GinkgoRecover()

				time.Sleep(listenerDelay)

				err := tcp.Listen(
					ctx,
					"127.0.0.1:3333",
					func(conn net.Conn) {
						defer GinkgoRecover()

						// Assume that the dialer will send a message first, and
						// try to receive the message.
						received := [100]byte{}
						n, err := io.ReadFull(conn, received[:])

						// Check that the expected message was received,
						Expect(n).To(Equal(100))
						Expect(err).ToNot(HaveOccurred())
						Expect(received[:]).To(Equal(message[:]))
						messageReceived <- received

						// and then send it back.
						n, err = conn.Write(message[:])
						Expect(n).To(Equal(100))
						Expect(err).ToNot(HaveOccurred())
					},
					nil,
					allow,
				)
				Expect(err).To(Equal(context.Canceled))
			}()
		}

		// Delaying the dialer is useful to allow the listener to boot. This
		// delay might need to be longer than expected, depending on the CI
		// environment.
		time.Sleep(dialDelay)

		err := tcp.Dial(
			ctx,
			"127.0.0.1:3333",
			func(conn net.Conn) {
				defer GinkgoRecover()

				// Send a message and, if the listener is enabled, then expect
				// it to succeed. Otherwise, expect it to fail.
				n, err := conn.Write(message[:])
				Expect(n).To(Equal(100))
				Expect(err).ToNot(HaveOccurred())

				// Same thing with receiving messages.
				received := [100]byte{}
				n, err = io.ReadFull(conn, received[:])
				if !rejectInboundConns {
					Expect(n).To(Equal(100))
					Expect(err).ToNot(HaveOccurred())
					Expect(received[:]).To(Equal(message[:]))
					messageReceived <- received
				} else {
					Expect(err).To(HaveOccurred())
				}
			},
			nil,
			policy.ConstantTimeout(time.Millisecond))

		// If the listener is enabled, and there is no policy for rejecting
		// inbound connection attempts, then we expect messages to have been
		// sent and received.
		if enableListener && !rejectInboundConns {
			Expect(err).ToNot(HaveOccurred())
			Expect(<-messageReceived).To(Equal(message))
			Expect(<-messageReceived).To(Equal(message))
		} else if rejectInboundConns {
			Expect(err).ToNot(HaveOccurred())
		} else {
			Expect(err).To(HaveOccurred())
		}
	}

	Context("when dialing a listener that is accepting inbound connections", func() {
		It("should send and receive messages", func() {
			for iter := 0; iter < 3; iter++ {
				func() {
					// Allow some time for the previous iteration to shutdown.
					defer time.Sleep(10 * time.Millisecond)

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					run(
						ctx,
						time.Millisecond, // Delay before dialing
						time.Duration(0), // Delay before listening
						true,             // Enable listening
						false,            // No listening policy
					)
				}()
			}
		})
	})

	Context("when dialing a listener that is not accepting inbound connections", func() {
		It("should return an error", func() {
			for iter := 0; iter < 3; iter++ {
				func() {
					// Allow some time for the previous iteration to shutdown.
					defer time.Sleep(10 * time.Millisecond)

					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					run(
						ctx,
						time.Millisecond, // Delay before dialing
						time.Duration(0), // Delay before listening
						false,            // Disable listening
						false,            // No listening policy
					)
				}()
			}
		})

		Context("when the listener begins accepting inbound connections", func() {
			It("should begin sending and receiving messages", func() {
				for iter := 0; iter < 3; iter++ {
					func() {
						// Allow some time for the previous iteration to shutdown.
						defer time.Sleep(10 * time.Millisecond)

						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						run(
							ctx,
							time.Millisecond, // Delay before dialing
							time.Second,      // Delay before listening
							true,             // Enable listening
							false,            // No listening policy
						)
					}()
				}
			})
		})
	})

	Context("when dialing a listener that is rejecting inbound connections", func() {
		It("should return an error", func() {
			for iter := 0; iter < 3; iter++ {
				func() {
					// Allow some time for the previous iteration to shutdown.
					defer time.Sleep(10 * time.Millisecond)

					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					run(
						ctx,
						time.Millisecond, // Delay before dialing
						time.Second,      // Delay before listening
						true,             // Enable listening
						true,             // No listening policy
					)
				}()
			}
		})
	})
})
