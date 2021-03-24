package peer_test

import (
	"context"
	"fmt"
	"github.com/renproject/aw/peer"
	"time"

	"github.com/renproject/aw/wire"
	"github.com/renproject/id"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Sync", func() {
	Context("when trying to sync valid content id on demand with nil hint", func() {
		It("should successfully receive corresponding message", func() {

			n := 2
			opts, peers, tables, contentResolvers, _, transports := setup(n)

			for i := range opts {
				opts[i].SyncerOptions = opts[i].SyncerOptions.WithWiggleTimeout(2 * time.Second)
				peers[i] = peer.New(
					opts[i],
					transports[i])
				peers[i].Resolve(context.Background(), contentResolvers[i])
			}

			tables[0].AddPeer(peers[1].ID(), wire.NewUnsignedAddress(wire.TCP,
				fmt.Sprintf("%v:%v", "localhost", uint16(3333+1)), uint64(time.Now().UnixNano())))
			tables[1].AddPeer(peers[0].ID(), wire.NewUnsignedAddress(wire.TCP,
				fmt.Sprintf("%v:%v", "localhost", uint16(3333)), uint64(time.Now().UnixNano())))

			for i := range peers {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				go peers[i].Run(ctx)
			}

			helloMsg := "Hi from peer 0"
			contentID := id.NewHash([]byte(helloMsg))
			contentResolvers[0].InsertContent(contentID[:], []byte(helloMsg))

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			msg, err := peers[1].Sync(ctx, contentID[:], nil)
			for err != nil {
				select {
				case <-ctx.Done():
					panic("Timeout expired before content was synced")
				default:
				}
				msg, err = peers[1].Sync(ctx, contentID[:], nil)
			}

			Ω(err).To(BeNil())
			Ω(msg).To(Equal([]byte(helloMsg)))
		})
	})

	Context("when getting a successful sync response on sending multiple parallel sync requests", func() {
		It("should not drop connections for additional sync responses", func() {

			n := 5
			opts, peers, tables, contentResolvers, _, transports := setup(n)

			for i := range opts {
				opts[i].SyncerOptions = opts[i].SyncerOptions.WithWiggleTimeout(2 * time.Second)
				peers[i] = peer.New(
					opts[i],
					transports[i])
				peers[i].Resolve(context.Background(), contentResolvers[i])
			}

			for i := range peers {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				go peers[i].Run(ctx)

				for j := range peers {
					if i != j {
						tables[i].AddPeer(opts[j].PrivKey.Signatory(),
							wire.NewUnsignedAddress(wire.TCP,
								fmt.Sprintf("%v:%v", "localhost", uint16(3333+j)), uint64(time.Now().UnixNano())))
					}
					helloMsg := fmt.Sprintf("Hello from peer %d", j)
					contentID := id.NewHash([]byte(helloMsg))
					contentResolvers[i].InsertContent(contentID[:], []byte(helloMsg))
				}
			}

			for i := range peers {
				for j := range peers {
					helloMsg := fmt.Sprintf("Hello from peer %d", j)
					contentID := id.NewHash([]byte(helloMsg))

					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					defer cancel()
					msg, err := peers[i].Sync(ctx, contentID[:], nil)

					for {
						if err == nil {
							break
						}
						select {
						case <-ctx.Done():
							break
						default:
							msg, err = peers[i].Sync(ctx, contentID[:], nil)
						}
					}

					Ω(msg).To(Equal([]byte(helloMsg)))
				}
			}
		})
	})

	Context("if a sync request fails", func() {
		It("the corresponding pending content condition variable should be deleted", func() {
			n := 2
			opts, peers, tables, contentResolvers, _, transports := setup(n)

			tables[0].AddPeer(opts[1].PrivKey.Signatory(),
				wire.NewUnsignedAddress(wire.TCP,
					fmt.Sprintf("%v:%v", "localhost", uint16(3333+1)), uint64(time.Now().UnixNano())))
			tables[1].AddPeer(opts[0].PrivKey.Signatory(),
				wire.NewUnsignedAddress(wire.TCP,
					fmt.Sprintf("%v:%v", "localhost", uint16(3333)), uint64(time.Now().UnixNano())))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			go peers[0].Run(ctx)
			go func(ctx context.Context) {
				once := false
				transports[1].Receive(ctx, func(from id.Signatory, msg wire.Msg) error {
					if !once {
						once = true
						return nil
					}

					if err := peers[1].Syncer().DidReceiveMessage(from, msg); err != nil {
						return err
					}
					if err := peers[1].Gossiper().DidReceiveMessage(from, msg); err != nil {
						return err
					}
					return nil
				})
				transports[1].Run(ctx)
			}(ctx)

			helloMsg := "Hello World!"
			contentID := id.NewHash([]byte(helloMsg))
			contentResolvers[0].InsertContent(contentID[:], []byte(helloMsg))

			func() {
				syncCtx, syncCancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer syncCancel()
				msg, err := peers[1].Sync(syncCtx, contentID[:], nil)
				Expect(msg).To(BeNil())
				Expect(err).To(Not(BeNil()))
			}()

			func() {
				syncCtx, syncCancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer syncCancel()
				msg, err := peers[1].Sync(syncCtx, contentID[:], nil)
				for err != nil {
					select {
					case <-syncCtx.Done():
						panic("Timeout expired before content was synced")
					default:
					}
					msg, err = peers[1].Sync(ctx, contentID[:], nil)
				}
				Ω(err).To(BeNil())
				Ω(msg).To(Equal([]byte(helloMsg)))
			}()

		})
	})
})
