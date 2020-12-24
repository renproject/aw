package channel_test

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/renproject/aw/channel"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Client", func() {

	sink := func(ctx context.Context, client *channel.Client, remote id.Signatory, n uint64) <-chan struct{} {
		quit := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(quit)
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			for iter := uint64(0); iter < n; iter++ {
				data := [8]byte{}
				binary.BigEndian.PutUint64(data[:], iter)
				err := client.Send(ctx, remote, wire.Msg{Data: data[:]})
				Expect(err).ToNot(HaveOccurred())
			}
		}()
		return quit
	}

	stream := func(ctx context.Context, client *channel.Client, n uint64) <-chan struct{} {
		quit := make(chan struct{})
		go func() {
			defer GinkgoRecover()
			defer close(quit)
			defer time.Sleep(time.Millisecond) // Wait for the receiver to be shutdown.
			ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			defer cancel()
			client.Receive(ctx, func(signatory id.Signatory, msg wire.Msg) error {
				return nil
			})
			//for iter := uint64(0); iter < n; iter++ {
			//	time.Sleep(time.Millisecond)
			//	select {
			//	case <-ctx.Done():
			//		Expect(ctx.Err()).ToNot(HaveOccurred())
			//	case msg := <-receiver:
			//		data := binary.BigEndian.Uint64(msg.Data)
			//		Expect(data).To(Equal(iter))
			//	}
			//}
		}()
		return quit
	}

	Context("when binding and attaching", func() {
		It("should send and receive all messages in order", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			localPrivKey := id.NewPrivKey()
			remotePrivKey := id.NewPrivKey()

			local := channel.NewClient(
				channel.DefaultOptions(),
				localPrivKey.Signatory())
			local.Bind(remotePrivKey.Signatory())
			defer local.Unbind(remotePrivKey.Signatory())
			Expect(local.IsBound(remotePrivKey.Signatory())).To(BeTrue())

			remote := channel.NewClient(
				channel.DefaultOptions(),
				remotePrivKey.Signatory())
			remote.Bind(localPrivKey.Signatory())
			defer remote.Unbind(localPrivKey.Signatory())
			Expect(remote.IsBound(localPrivKey.Signatory())).To(BeTrue())

			listen(ctx, remote, remotePrivKey.Signatory(), localPrivKey.Signatory(), 4444)
			dial(ctx, local, localPrivKey.Signatory(), remotePrivKey.Signatory(), 4444, time.Minute)

			n := uint64(5000)
			q1 := sink(ctx, local, remotePrivKey.Signatory(), n)
			q2 := stream(ctx, remote, n)
			q3 := sink(ctx, remote, localPrivKey.Signatory(), n)
			q4 := stream(ctx, local, n)

			<-q1
			<-q2
			<-q3
			<-q4
		})
	})

	Context("when binding and unbinding while attached", func() {
		It("should send and receive all messages in order", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			localPrivKey := id.NewPrivKey()
			remotePrivKey := id.NewPrivKey()

			local := channel.NewClient(
				channel.DefaultOptions(),
				localPrivKey.Signatory())
			local.Bind(remotePrivKey.Signatory())
			defer local.Unbind(remotePrivKey.Signatory())
			Expect(local.IsBound(remotePrivKey.Signatory())).To(BeTrue())

			remote := channel.NewClient(
				channel.DefaultOptions(),
				remotePrivKey.Signatory())
			remote.Bind(localPrivKey.Signatory())
			defer remote.Unbind(localPrivKey.Signatory())
			Expect(remote.IsBound(localPrivKey.Signatory())).To(BeTrue())

			listen(ctx, remote, remotePrivKey.Signatory(), localPrivKey.Signatory(), 4444)
			dial(ctx, local, localPrivKey.Signatory(), remotePrivKey.Signatory(), 4444, time.Minute)

			go func() {
				remote := remotePrivKey.Signatory()
				for {
					select {
					case <-ctx.Done():
						return
					default:
						local.Bind(remote)
						time.Sleep(time.Millisecond)
						local.Unbind(remote)
						time.Sleep(time.Millisecond)
					}
				}
			}()

			go func() {
				local := localPrivKey.Signatory()
				for {
					select {
					case <-ctx.Done():
						return
					default:
						remote.Bind(local)
						time.Sleep(time.Millisecond)
						remote.Unbind(local)
						time.Sleep(time.Millisecond)
					}
				}
			}()

			n := uint64(5000)
			q1 := sink(ctx, local, remotePrivKey.Signatory(), n)
			q2 := stream(ctx, remote, n)
			q3 := sink(ctx, remote, localPrivKey.Signatory(), n)
			q4 := stream(ctx, local, n)

			<-q1
			<-q2
			<-q3
			<-q4
		})
	})

	Context("when sending before binding", func() {
		It("should return an error", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			remotePrivKey := id.NewPrivKey()
			localPrivKey := id.NewPrivKey()
			local := channel.NewClient(
				channel.DefaultOptions(),
				localPrivKey.Signatory())
			Expect(local.Send(ctx, remotePrivKey.Signatory(), wire.Msg{})).To(HaveOccurred())
		})
	})

	Context("when attaching before binding", func() {
		It("should return an error", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			remotePrivKey := id.NewPrivKey()
			localPrivKey := id.NewPrivKey()
			local := channel.NewClient(
				channel.DefaultOptions(),
				localPrivKey.Signatory())
			Expect(local.Attach(ctx, remotePrivKey.Signatory(), nil, nil, nil)).To(HaveOccurred())
		})
	})
})
