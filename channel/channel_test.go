package channel_test

import (
	"context"
	"encoding/binary"
	"log"
	"math/rand"
	"time"

	"github.com/renproject/aw/channel"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Channels", func() {

	run := func(ctx context.Context, remote id.Signatory) (*channel.Channel, <-chan wire.Msg, chan<- wire.Msg) {
		inbound, outbound := make(chan wire.Msg), make(chan wire.Msg)
		ch := channel.New(
			channel.DefaultOptions().WithDrainTimeout(1500*time.Millisecond),
			remote,
			inbound,
			outbound)
		go func() {
			defer GinkgoRecover()
			if err := ch.Run(ctx); err != nil {
				log.Printf("run: %v", err)
				return
			}
		}()
		return ch, inbound, outbound
	}

	sink := func(outbound chan<- wire.Msg, n uint64) <-chan struct{} {
		quit := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(quit)
			timeout := time.After(30 * time.Second)
			for iter := uint64(0); iter < n; iter++ {
				time.Sleep(time.Millisecond)
				data := [8]byte{}
				binary.BigEndian.PutUint64(data[:], iter)
				select {
				case outbound <- wire.Msg{Data: data[:]}:
				case <-timeout:
					Expect(func() { panic("sink timeout") }).ToNot(Panic())
				}
			}
		}()
		return quit
	}

	stream := func(inbound <-chan wire.Msg, n uint64, inOrder bool) <-chan struct{} {
		quit := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(quit)
			timeout := time.After(30 * time.Second)
			max := uint64(0)
			received := make(map[uint64]int, n)
			for iter := uint64(0); iter < n; iter++ {
				select {
				case msg := <-inbound:
					data := binary.BigEndian.Uint64(msg.Data)
					if data > max {
						max = data
					}
					received[data]++
					if inOrder {
						Expect(data).To(Equal(iter))
					}
					if rand.Int()%1000 == 0 {
						log.Printf("stream %v/%v", len(received), max+1)
					}
				case <-timeout:
					Expect(func() { panic("stream timeout") }).ToNot(Panic())
				}
			}
			for msg, count := range received {
				if count > 1 {
					log.Printf("duplicate %v (%v)", msg, count)
				}
				Expect(count).To(BeNumerically(">=", 1))
			}
			Expect(len(received)).To(Equal(int(n)))
		}()
		return quit
	}

	Context("when a connection is attached before sending messages", func() {
		It("should send and receive all message in order", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			localPrivKey := id.NewPrivKey()
			remotePrivKey := id.NewPrivKey()
			localCh, localInbound, localOutbound := run(ctx, remotePrivKey.Signatory())
			remoteCh, remoteInbound, remoteOutbound := run(ctx, localPrivKey.Signatory())

			// Remote channel will listen for incoming connections.
			port := listen(ctx, remoteCh, remotePrivKey.Signatory(), localPrivKey.Signatory())
			// Local channel will dial the listener (and re-dial once per
			// minute; so it should not impact the test, which is expected
			// to complete in less than one minute).
			dial(ctx, localCh, localPrivKey.Signatory(), remotePrivKey.Signatory(), port, time.Minute)

			// Wait for the connections to be attached before beginning to
			// send/receive messages.
			time.Sleep(time.Second)

			// Number of messages that we will test.
			n := uint64(1000)
			// Send and receive messages in both direction; from local to
			// remote, and from remote to local.
			q1 := sink(localOutbound, n)
			q2 := stream(remoteInbound, n, true)
			q3 := sink(remoteOutbound, n)
			q4 := stream(localInbound, n, true)

			<-q1
			<-q2
			<-q3
			<-q4
		})
	})

	Context("when a connection is attached after sending messages", func() {
		It("should send and receive all message in order", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			localPrivKey := id.NewPrivKey()
			remotePrivKey := id.NewPrivKey()
			localCh, localInbound, localOutbound := run(ctx, remotePrivKey.Signatory())
			remoteCh, remoteInbound, remoteOutbound := run(ctx, localPrivKey.Signatory())

			// Number of messages that we will test.
			n := uint64(1000)
			// Send and receive messages in both direction; from local to
			// remote, and from remote to local.
			q1 := sink(localOutbound, n)
			q2 := stream(remoteInbound, n, true)
			q3 := sink(remoteOutbound, n)
			q4 := stream(localInbound, n, true)

			// Wait for some messages to begin being sent/received before
			// attaching network connections.
			time.Sleep(time.Second)

			// Remote channel will listen for incoming connections.
			port := listen(ctx, remoteCh, remotePrivKey.Signatory(), localPrivKey.Signatory())
			// Local channel will dial the listener (and re-dial once per
			// minute; so it should not impact the test, which is expected
			// to complete in less than one minute).
			dial(ctx, localCh, localPrivKey.Signatory(), remotePrivKey.Signatory(), port, time.Minute)

			<-q1
			<-q2
			<-q3
			<-q4
		})
	})

	Context("when a connection is replaced while sending messages", func() {
		Context("when draining connections in the background", func() {
			It("should send and receive all messages out of order", func() {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				localPrivKey := id.NewPrivKey()
				remotePrivKey := id.NewPrivKey()
				localCh, localInbound, localOutbound := run(ctx, remotePrivKey.Signatory())
				remoteCh, remoteInbound, remoteOutbound := run(ctx, localPrivKey.Signatory())

				// Number of messages that we will test. This number is higher than
				// in other tests, because we need sending/receiving to take long
				// enough that replacements will happen.
				n := uint64(10000)
				// Send and receive messages in both direction; from local to
				// remote, and from remote to local.
				q1 := sink(localOutbound, n)
				q2 := stream(remoteInbound, n, false)
				q3 := sink(remoteOutbound, n)
				q4 := stream(localInbound, n, false)

				// Remote channel will listen for incoming connections.
				port := listen(ctx, remoteCh, remotePrivKey.Signatory(), localPrivKey.Signatory())
				// Local channel will dial the listener (and re-dial once per
				// second).
				dial(ctx, localCh, localPrivKey.Signatory(), remotePrivKey.Signatory(), port, time.Second)

				// Wait for sinking and streaming to finish.
				<-q1
				<-q2
				<-q3
				<-q4
			})
		})
	})
})
