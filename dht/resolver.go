package dht

import (
	"sync"
)

type ContentID [32]byte

// A ContentResolver interface allows for third-party content resolution. This
// can be used to persist content to the disk.
type ContentResolver interface {
	// Insert content with the given hash and type.
	Insert(ContentID, []byte)

	// Delete content with the given hash and type.
	Delete(ContentID)

	// Content returns the content associated with a hash. If there is no
	// associated content, it returns false. Otherwise, it returns true.
	Content(ContentID) ([]byte, bool)
}

var (
	// DefaultDoubleCacheContentResolverCapacity defines the default in-memory
	// cache capacity (in bytes) for the double-cache content resolver.
	DefaultDoubleCacheContentResolverCapacity = 128 * 1024 * 1024 // 128 MB
)

// DoubleCacheContentResolverOptions for parameterising the behaviour of the
// DoubleCacheContentResolver.
type DoubleCacheContentResolverOptions struct {
	Capacity int
}

// DefaultDoubleCacheContentResolverOptions returns the default
// DoubleCacheContentResolverOptions.
func DefaultDoubleCacheContentResolverOptions() DoubleCacheContentResolverOptions {
	return DoubleCacheContentResolverOptions{
		Capacity: DefaultDoubleCacheContentResolverCapacity,
	}
}

// WithCapacity sets the maximum in-memory cache capacity (in bytes). This
// capacity accounts for the fact that the double-cache content resolver has two
// in-memory buffers. For example, if the capacity is set to 2 MB, then the
// double-cache content resolver is guaranteeed to consume, at most, 2 MB of
// memory, but will only be able to cache 1 MB of data.
func (opts DoubleCacheContentResolverOptions) WithCapacity(capacity int) DoubleCacheContentResolverOptions {
	opts.Capacity = capacity / 2
	return opts
}

// The DoubleCacheContentResolver uses the double-cache technique to implement a
// fast in-memory cache. The cache can optionally wrap around another
// content-resolver (which can be responsible for more persistent content
// resolution).
type DoubleCacheContentResolver struct {
	opts DoubleCacheContentResolverOptions
	next ContentResolver

	cacheMu        *sync.Mutex
	cacheFrontSize int
	cacheFront     map[ContentID][]byte // Front is used to add new content until the max capacity is reached.
	cacheBack      map[ContentID][]byte // Back is used to read old content that has been rotated from the front.
}

// NewDoubleCacheContentResolver returns a new double-cache content resolver
// that is wrapped around another content-resolver.
func NewDoubleCacheContentResolver(opts DoubleCacheContentResolverOptions, next ContentResolver) *DoubleCacheContentResolver {
	return &DoubleCacheContentResolver{
		opts: opts,
		next: next,

		cacheMu:        new(sync.Mutex),
		cacheFrontSize: 0,
		cacheFront:     make(map[ContentID][]byte, 0),
		cacheBack:      make(map[ContentID][]byte, 0),
	}
}

// Insert content into the double-cache content resolver. If the front cache is
// full, it will be rotated to the back, the current back cache will be dropped,
// and a new front cache will be created. This method will also insert the
// content to the next content resovler (if one exists).
func (r *DoubleCacheContentResolver) Insert(id ContentID, content []byte) {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()

	// We cannot cache something that is greater than the maximum capacity.
	if len(content) > r.opts.Capacity {
		if r.next != nil {
			r.next.Insert(id, content)
		}
		return
	}

	// If the capacity has been exceeded, move the front cache to the back and
	// reset the front cache.
	if r.cacheFrontSize+len(content) > r.opts.Capacity {
		r.cacheBack = r.cacheFront
		r.cacheFrontSize = 0
		r.cacheFront = make(map[ContentID][]byte, 0)
	}

	// Insert the content to the front cache and the next resolver (if it
	// exists).
	r.cacheFrontSize += len(content)
	r.cacheFront[id] = content

	if r.next != nil {
		r.next.Insert(id, content)
	}
}

// Delete content from the double-cache content resolver. This method will also
// delete the content from the next content resolver (if one exists).
func (r *DoubleCacheContentResolver) Delete(id ContentID) {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()

	// Delete the content from both caches and the next resolver (if it
	// exists).
	if content, ok := r.cacheFront[id]; ok {
		r.cacheFrontSize -= len(content)
		delete(r.cacheFront, id)
	}
	delete(r.cacheBack, id)

	if r.next != nil {
		r.next.Delete(id)
	}
}

// Content returns the content associated with the given hash. If the content is
// not found in the double-cache content resolver, the next content resolver
// will be checked (if one exists).
func (r *DoubleCacheContentResolver) Content(id ContentID) ([]byte, bool) {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()

	// Check both caches for the content.
	if content, ok := r.cacheFront[id]; ok {
		return content, ok
	}
	if content, ok := r.cacheBack[id]; ok {
		return content, ok
	}

	// If the content has not been found, check the next resolver.
	if r.next != nil {
		return r.next.Content(id)
	}
	return nil, false
}

// CallbackContentResolver implements the ContentResolve interface by delegating
// all logic to callback functions. This is useful when defining an
// implementation inline.
type CallbackContentResolver struct {
	InsertCallback  func(ContentID, []byte)
	DeleteCallback  func(ContentID)
	ContentCallback func(ContentID) ([]byte, bool)
}

// Insert will delegate the implementation to the InsertCallback. If the
// callback is nil, then this method will do nothing.
func (r CallbackContentResolver) Insert(id ContentID, content []byte) {
	if r.InsertCallback != nil {
		r.InsertCallback(id, content)
	}
}

// Delete will delegate the implementation to the DeleteCallback. If the
// callback is nil, then this method will do nothing.
func (r CallbackContentResolver) Delete(id ContentID) {
	if r.DeleteCallback != nil {
		r.DeleteCallback(id)
	}
}

// Content will delegate the implementation to the ContentCallback. If the
// callback is nil, then this method will return false.
func (r CallbackContentResolver) Content(id ContentID) ([]byte, bool) {
	if r.ContentCallback != nil {
		return r.ContentCallback(id)
	}
	return nil, false
}
