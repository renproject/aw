package dht

var (
	// DefaultMaxRandomSignatories is set to 10.
	DefaultMaxRandomSignatories = 10
)

// Options to configure the behaviour of the DHT.
type Options struct {
	MaxRandomSignatories int
}

// DefaultOptions returns new Options with sane defaults.
func DefaultOptions() Options {
	return Options{
		MaxRandomSignatories: DefaultMaxRandomSignatories,
	}
}

// WithMaxRandomSignatories returns new Options with the given maximum random
// signatories. This is the maximum number of random signatories that will be
// returned when querying a subnet using the DefaultHash.
func (opts Options) WithMaxRandomSignatories(maxRandomSignatories int) Options {
	opts.MaxRandomSignatories = maxRandomSignatories
	return opts
}
