package dht_test

import (
	"crypto/sha256"
	"math/rand"
	"testing/quick"
	"time"

	"github.com/renproject/aw/dht"
	"github.com/renproject/id"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Content Resolver", func() {
	Describe("Double-Cache", func() {
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

			It("should drop old values", func() {
				capacity := 10
				resolver := dht.NewDoubleCacheContentResolver(
					dht.DefaultDoubleCacheContentResolverOptions().
						WithCapacity(capacity),
					nil,
				)

				// Fill cache with data.
				hashes := make([]id.Hash, capacity)
				for i := 0; i < capacity; i++ {
					content := randomContent()
					hashes[i] = id.Hash(sha256.Sum256(content))
					resolver.Insert(hashes[i], uint8(i), content)
				}

				// Add more data to cause old data to be dropped.
				newHashes := make([]id.Hash, capacity)
				newContent := make([][]byte, capacity)
				for i := 0; i < capacity; i++ {
					newContent[i] = randomContent()
					newHashes[i] = id.Hash(sha256.Sum256(newContent[i]))
					resolver.Insert(newHashes[i], uint8(i), newContent[i])
				}

				// Verify new data exists and old data has been dropped.
				for _, hash := range hashes {
					content, ok := resolver.Content(hash)
					Expect(ok).To(BeFalse())
					Expect(len(content)).To(Equal(0))
				}

				for i := range newHashes {
					content, ok := resolver.Content(newHashes[i])
					Expect(ok).To(BeTrue())
					Expect(content).To(Equal(newContent[i]))
				}
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
					NewMockResolver(insertCh, deleteCh, contentCh),
				)

				// Insert.
				hash := id.Hash(sha256.Sum256(randomContent()))
				go resolver.Insert(hash, 0, nil)

				newHash := <-insertCh
				Expect(newHash).To(Equal(hash))

				// Delete.
				hash = id.Hash(sha256.Sum256(randomContent()))
				go resolver.Delete(hash)

				newHash = <-deleteCh
				Expect(newHash).To(Equal(hash))

				// Get.
				hash = id.Hash(sha256.Sum256(randomContent()))
				go resolver.Content(hash)

				newHash = <-contentCh
				Expect(newHash).To(Equal(hash))

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
})

// TODO: Migrate this to a util package.
func randomContent() []byte {
	content := make([]byte, 32)
	_, err := rand.Read(content)
	Expect(err).ToNot(HaveOccurred())
	return content
}

type mockResolver struct {
	insertCh  chan id.Hash
	deleteCh  chan id.Hash
	contentCh chan id.Hash
}

func NewMockResolver(insertCh, deleteCh, contentCh chan id.Hash) dht.ContentResolver {
	return &mockResolver{
		insertCh:  insertCh,
		deleteCh:  deleteCh,
		contentCh: contentCh,
	}
}

func (r *mockResolver) Insert(hash id.Hash, contentType uint8, content []byte) {
	r.insertCh <- hash
}

func (r *mockResolver) Delete(hash id.Hash) {
	r.deleteCh <- hash
}

func (r *mockResolver) Content(hash id.Hash) ([]byte, bool) {
	r.contentCh <- hash
	return nil, true
}
