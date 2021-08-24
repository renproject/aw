package dht

import (
	"sync"
)

// The ContentResolver interface is used to insert and query content.
type ContentResolver interface {
	// Insert content with a specific content ID. Usually, the content ID will
	// stores information about the type and the hash of the content.
	InsertContent(contentID, content []byte)

	// QueryContent returns the content associated with a content ID. If there
	// is no associated content, it returns false. Otherwise, it returns true.
	QueryContent(contentID []byte) (content []byte, contentOk bool)
}

var (
	// DefaultDoubleCacheContentResolverCapacity defines the default in-memory
	// cache capacity (in bytes) for the double-cache content resolver.
	DefaultDoubleCacheContentResolverCapacity = 16 * 1024 * 1024 // 16 MB
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
	cacheFront     map[string][]byte // Front is used to add new content until the max capacity is reached.
	cacheBack      map[string][]byte // Back is used to read old content that has been rotated from the front.
}

// NewDoubleCacheContentResolver returns a new double-cache content resolver
// that is wrapped around another content-resolver.
func NewDoubleCacheContentResolver(opts DoubleCacheContentResolverOptions, next ContentResolver) *DoubleCacheContentResolver {
	return &DoubleCacheContentResolver{
		opts: opts,
		next: next,

		cacheMu:        new(sync.Mutex),
		cacheFrontSize: 0,
		cacheFront:     make(map[string][]byte, 0),
		cacheBack:      make(map[string][]byte, 0),
	}
}

// InsertContent into the double-cache content resolver. If the front cache is
// full, it will be rotated to the back, the current back cache will be dropped,
// and a new front cache will be created. This method will also insert the
// content to the next content resovler (if one exists).
func (r *DoubleCacheContentResolver) InsertContent(id, content []byte) {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()

	// We cannot cache something that is greater than the maximum capacity.
	if len(content) > r.opts.Capacity {
		if r.next != nil {
			r.next.InsertContent(id, content)
		}
		return
	}

	// If the capacity has been exceeded, move the front cache to the back and
	// reset the front cache.
	if r.cacheFrontSize+len(content) > r.opts.Capacity {
		r.cacheBack = r.cacheFront
		r.cacheFrontSize = 0
		r.cacheFront = make(map[string][]byte, 0)
	}

	// Insert the content to the front cache and the next resolver (if it
	// exists).
	r.cacheFrontSize += len(content)
	r.cacheFront[string(id)] = content

	if r.next != nil {
		r.next.InsertContent(id, content)
	}
}

// QueryContent returns the content associated with the given content ID. If the
// content is not found in the double-cache content resolver, the next content
// resolver will be checked (if one exists).
func (r *DoubleCacheContentResolver) QueryContent(id []byte) ([]byte, bool) {
	r.cacheMu.Lock()
	defer r.cacheMu.Unlock()

	// Check both caches for the content.
	if content, ok := r.cacheFront[string(id)]; ok {
		return content, ok
	}
	if content, ok := r.cacheBack[string(id)]; ok {
		return content, ok
	}

	// If the content has not been found, check the next resolver.
	if r.next != nil {
		return r.next.QueryContent(id)
	}
	return nil, false
}

// CallbackContentResolver implements the ContentResolve interface by delegating
// all logic to callback functions. This is useful when defining an
// implementation inline.
type CallbackContentResolver struct {
	InsertContentCallback func([]byte, []byte)
	QueryContentCallback  func([]byte) ([]byte, bool)
}

// InsertContent will delegate the implementation to the InsertContentCallback.
// If the callback is nil, then this method will do nothing.
func (r CallbackContentResolver) InsertContent(id, content []byte) {
	if r.InsertContentCallback != nil {
		r.InsertContentCallback(id, content)
	}
}

// QueryContent will delegate the implementation to the QueryContentCallback. If
// the callback is nil, then this method will return false.
func (r CallbackContentResolver) QueryContent(id []byte) ([]byte, bool) {
	if r.QueryContentCallback != nil {
		return r.QueryContentCallback(id)
	}
	return nil, false
}
