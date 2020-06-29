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
				resolver.Insert(hash, contentType, content)

				newContent, ok := resolver.Content(hash)
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
			resolver.Insert(hash, 0, content[:])

			_, ok := resolver.Content(hash)
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
			resolver.Insert(hash, 0, content[:])

			// Add more data.
			newContent := [10]byte{0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19}
			newHash := id.NewHash(newContent[:])
			resolver.Insert(newHash, 0, newContent[:])

			// Both chunks of data should be present.
			_, ok := resolver.Content(hash)
			Expect(ok).To(BeTrue())
			_, ok = resolver.Content(newHash)
			Expect(ok).To(BeTrue())

			// Add event more data.
			newerContent := [10]byte{0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29}
			newerHash := id.NewHash(newerContent[:])
			resolver.Insert(newerHash, 0, newerContent[:])

			// Verify the two latest chunks exist, and that the rest has been
			// rotated out.
			_, ok = resolver.Content(hash)
			Expect(ok).To(BeFalse())
			_, ok = resolver.Content(newHash)
			Expect(ok).To(BeTrue())
			_, ok = resolver.Content(newerHash)
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
				newContent, ok := resolver.Content(hash)
				Expect(ok).To(BeFalse())
				Expect(len(newContent)).To(Equal(0))
				return true
			}
			Expect(quick.Check(f, nil)).To(Succeed())
		})
	})

	Context("when using an inner resolver", func() {
		It("should forward calls to it", func() {
			insertCh := make(chan id.Hash)
			deleteCh := make(chan id.Hash)
			contentCh := make(chan id.Hash)

			resolver := dht.NewDoubleCacheContentResolver(
				dht.DefaultDoubleCacheContentResolverOptions(),
				dhtutil.NewMockResolver(insertCh, deleteCh, contentCh),
			)

			// Insert and wait on the channel to make sure the inner
			// resolver received the message.
			hash := id.Hash(sha256.Sum256(dhtutil.RandomContent()))
			go resolver.Insert(hash, 0, nil)

			newHash := <-insertCh
			Expect(newHash).To(Equal(hash))

			// Delete and wait on the channel to make sure the inner
			// resolver received the message.
			hash = id.Hash(sha256.Sum256(dhtutil.RandomContent()))
			go resolver.Delete(hash)

			newHash = <-deleteCh
			Expect(newHash).To(Equal(hash))

			// Get and wait on the channel to make sure the inner resolver
			// received the message.
			hash = id.Hash(sha256.Sum256(dhtutil.RandomContent()))
			go resolver.Content(hash)

			newHash = <-contentCh
			Expect(newHash).To(Equal(hash))

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
			Expect(func() { dht.CallbackContentResolver{}.Insert(id.Hash{}, 0, []byte{}) }).ToNot(Panic())
			Expect(func() { dht.CallbackContentResolver{}.Delete(id.Hash{}) }).ToNot(Panic())
			Expect(func() { dht.CallbackContentResolver{}.Content(id.Hash{}) }).ToNot(Panic())
		})
	})

	Context("when callbacks are defined", func() {
		It("should delegate to the callback", func() {
			cond1 := false
			cond2 := false
			cond3 := false

			resolver := dht.CallbackContentResolver{
				InsertCallback: func(id.Hash, uint8, []byte) {
					cond1 = true
				},
				DeleteCallback: func(id.Hash) {
					cond2 = true
				},
				ContentCallback: func(id.Hash) ([]byte, bool) {
					cond3 = true
					return nil, false
				},
			}
			resolver.Insert(id.Hash{}, 0, []byte{})
			resolver.Delete(id.Hash{})
			resolver.Content(id.Hash{})

			Expect(cond1).To(BeTrue())
			Expect(cond2).To(BeTrue())
			Expect(cond3).To(BeTrue())
		})
	})
})
