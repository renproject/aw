package dht_test

import (
	"crypto/sha256"
	"testing/quick"
	"time"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/dht/dhtutil"
	"github.com/renproject/id"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Double-cache Content Resolver", func() {
	Context("when inserting content", func() {
		It("should be able to query it", func() {
			resolver := dht.NewDoubleCacheContentResolver(
				dht.DefaultDoubleCacheContentResolverOptions(),
				nil,
			)

			f := func(contentType uint8, content []byte) bool {
				hash := id.Hash(sha256.Sum256(content))
				resolver.Insert(hash[:], content)

				newContent, ok := resolver.Content(hash[:])
				Expect(ok).To(BeTrue())
				Expect(newContent).To(Equal(content))
				return true
			}
			Expect(quick.Check(f, nil)).To(Succeed())
		})

		It("should ignore content that is too big", func() {
			capacity := 19
			resolver := dht.NewDoubleCacheContentResolver(
				dht.DefaultDoubleCacheContentResolverOptions().
					WithCapacity(capacity),
				nil,
			)

			// Fill cache with data that is too big.
			content := [10]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}
			hash := id.NewHash(content[:])
			resolver.Insert(hash[:], content[:])

			_, ok := resolver.Content(hash[:])
			Expect(ok).To(BeFalse())
		})

		It("should drop old values", func() {
			capacity := 20
			resolver := dht.NewDoubleCacheContentResolver(
				dht.DefaultDoubleCacheContentResolverOptions().
					WithCapacity(capacity),
				nil,
			)

			// Fill cache with data.
			content := [10]byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09}
			hash := id.NewHash(content[:])
			resolver.Insert(hash[:], content[:])

			// Add more data.
			newContent := [10]byte{0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19}
			newHash := id.NewHash(newContent[:])
			resolver.Insert(newHash[:], newContent[:])

			// Both chunks of data should be present.
			_, ok := resolver.Content(hash[:])
			Expect(ok).To(BeTrue())
			_, ok = resolver.Content(newHash[:])
			Expect(ok).To(BeTrue())

			// Add event more data.
			newerContent := [10]byte{0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29}
			newerHash := id.NewHash(newerContent[:])
			resolver.Insert(newerHash[:], newerContent[:])

			// Verify the two latest chunks exist, and that the rest has been
			// rotated out.
			_, ok = resolver.Content(hash[:])
			Expect(ok).To(BeFalse())
			_, ok = resolver.Content(newHash[:])
			Expect(ok).To(BeTrue())
			_, ok = resolver.Content(newerHash[:])
			Expect(ok).To(BeTrue())
		})
	})

	Context("when querying content that does not exist", func() {
		It("should return false", func() {
			resolver := dht.NewDoubleCacheContentResolver(
				dht.DefaultDoubleCacheContentResolverOptions(),
				nil,
			)

			f := func(contentType uint8, content []byte) bool {
				hash := id.Hash(sha256.Sum256(content))
				newContent, ok := resolver.Content(hash[:])
				Expect(ok).To(BeFalse())
				Expect(len(newContent)).To(Equal(0))
				return true
			}
			Expect(quick.Check(f, nil)).To(Succeed())
		})
	})

	Context("when using an inner resolver", func() {
		It("should forward calls to it", func() {
			insertCh := make(chan []byte)
			deleteCh := make(chan []byte)
			contentCh := make(chan []byte)

			resolver := dht.NewDoubleCacheContentResolver(
				dht.DefaultDoubleCacheContentResolverOptions(),
				dht.CallbackContentResolver{
					InsertCallback: func(id []byte, data []byte) {
						insertCh <- id
					},
					DeleteCallback: func(id []byte) {
						deleteCh <- id
					},
					ContentCallback: func(id []byte) ([]byte, bool) {
						contentCh <- id
						return []byte{}, true
					},
				},
			)

			// Insert and wait on the channel to make sure the inner
			// resolver received the message.
			hash := id.Hash(sha256.Sum256(dhtutil.RandomContent()))
			go resolver.Insert(hash[:], nil)
			newHash := <-insertCh
			Expect(newHash).To(Equal(hash[:]))

			// Delete and wait on the channel to make sure the inner
			// resolver received the message.
			hash = sha256.Sum256(dhtutil.RandomContent())
			go resolver.Delete(hash[:])

			newHash = <-deleteCh
			Expect(newHash).To(Equal(hash[:]))

			// Get and wait on the channel to make sure the inner resolver
			// received the message.
			hash = sha256.Sum256(dhtutil.RandomContent())
			go resolver.Content(hash[:])

			newHash = <-contentCh
			Expect(newHash).To(Equal(hash[:]))

			// Ensure the channels receive no additional messages.
			select {
			case <-insertCh:
				Fail("unexpected insert message")
			case <-deleteCh:
				Fail("unexpected delete message")
			case <-contentCh:
				Fail("unexpected content message")
			case <-time.After(time.Second):
			}
		})
	})
})

var _ = Describe("Callback Content Resolver", func() {
	Context("when callbacks are not defined", func() {
		It("should not panic", func() {
			hash := id.Hash{}
			Expect(func() { dht.CallbackContentResolver{}.Insert(hash[:], []byte{}) }).ToNot(Panic())
			Expect(func() { dht.CallbackContentResolver{}.Delete(hash[:]) }).ToNot(Panic())
			Expect(func() { dht.CallbackContentResolver{}.Content(hash[:]) }).ToNot(Panic())
		})
	})

	Context("when callbacks are defined", func() {
		It("should delegate to the callback", func() {
			hash := id.Hash{}
			cond1 := false
			cond2 := false
			cond3 := false

			resolver := dht.CallbackContentResolver{
				InsertCallback: func([]byte, []byte) {
					cond1 = true
				},
				DeleteCallback: func([]byte) {
					cond2 = true
				},
				ContentCallback: func([]byte) ([]byte, bool) {
					cond3 = true
					return nil, false
				},
			}
			resolver.Insert(hash[:], []byte{})
			resolver.Delete(hash[:])
			resolver.Content(hash[:])

			Expect(cond1).To(BeTrue())
			Expect(cond2).To(BeTrue())
			Expect(cond3).To(BeTrue())
		})
	})
})
