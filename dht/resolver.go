package dht

import (
	"sync"

	"github.com/renproject/id"
)

// A ContentResolver interface allows for third-party content resolution. This
// can be used to persist content to the disk.
type ContentResolver interface {
	// Insert content with the given hash and type.
	Insert(id.Hash, uint8, []byte)

	// Delete content with the given hash.
	Delete(id.Hash)

	// Content returns the content associated with a hash. If there is no
	// associated content, it returns false. Otherwise, it returns true.
	Content(id.Hash) ([]byte, bool)
}

var (
	DefaultDoubleCacheContentResolverCapacity = 256
)

type DoubleCacheContentResolverOptions struct {
	Capacity int
}

func DefaultDoubleCacheContentResolverOptions() DoubleCacheContentResolverOptions {
	return DoubleCacheContentResolverOptions{
		Capacity: DefaultDoubleCacheContentResolverCapacity,
	}
}

func (opts DoubleCacheContentResolverOptions) WithCapacity(capacity int) DoubleCacheContentResolverOptions {
	// Halve the value provided as there are two caches.
	opts.Capacity = capacity / 2
	return opts
}

type doubleCacheContentResolver struct {
	opts  DoubleCacheContentResolverOptions
	inner ContentResolver

	cacheMu    *sync.Mutex
	cacheFront map[id.Hash][]byte // Front is used to add new content until the max capacity is reached.
	cacheBack  map[id.Hash][]byte // Back is used to read old content that has been rotated from the front.
}

// NewDoubleCacheContentResolver returns a new double-cache content resolver
// wrapped around a given resolver.
func NewDoubleCacheContentResolver(opts DoubleCacheContentResolverOptions, inner ContentResolver) ContentResolver {
	return &doubleCacheContentResolver{
		opts:  opts,
		inner: inner,

		cacheMu:    new(sync.Mutex),
		cacheFront: make(map[id.Hash][]byte, opts.Capacity),
		cacheBack:  make(map[id.Hash][]byte, 0),
	}
}

func (r *doubleCacheContentResolver) Insert(hash id.Hash, contentType uint8, content []byte) {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()

	// If the capacity has been exceeded, move the front cache to the back and
	// reset the front cache.
	if len(r.cacheFront) >= r.opts.Capacity {
		r.cacheBack = r.cacheFront
		r.cacheFront = make(map[id.Hash][]byte, r.opts.Capacity)
	}

	// Insert the content to the front cache and the inner resolver (if it
	// exists).
	r.cacheFront[hash] = content

	if r.inner != nil {
		r.inner.Insert(hash, contentType, content)
	}
}

func (r *doubleCacheContentResolver) Delete(hash id.Hash) {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()

	// Delete the content from both caches and the inner resolver (if it
	// exists).
	delete(r.cacheFront, hash)
	delete(r.cacheBack, hash)

	if r.inner != nil {
		r.inner.Delete(hash)
	}
}

func (r *doubleCacheContentResolver) Content(hash id.Hash) ([]byte, bool) {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()

	// Check both caches for the content.
	if content, ok := r.cacheFront[hash]; ok {
		return content, ok
	}
	if content, ok := r.cacheBack[hash]; ok {
		return content, ok
	}

	// If the content has not been found, check the inner resolver.
	if r.inner != nil {
		return r.inner.Content(hash)
	}
	return nil, false
}
