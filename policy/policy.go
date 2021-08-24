// Package policy defines functions that control which connections are allowed,
// and which ones are denied. For server-side imposed policies, Allow functions
// are used to filter connections. For client-side imposed policies, Timeout
// functions are used to filter connections.
//
// When listening for incoming connections, it is impossible to control the
// conditions under which the remote client attempts to dial a new connection.
// As such, more powerful policy functions are required. The opposite is true
// for the dialer; when dialing a new connection to a remote peer, it can be
// assumed that the application-level logic will avoid dialing under conditions
// that the dialer does not find desirable. Here, the Timeout functions are the
// client-side analogy to the Allow functions. Although the Timeout functions
// are much less powerful, they are sufficient for dialers.
//
// Policy functions (Allow functions and Timeout functions) are built in using a
// functional style, and are designed to be composed together. In this way,
// policy functions are able to define only a small and simple (and easily
// testable) amounts of filtering logic, but still be composed together to
// create more complex filtering logic.
//
//	// Create a policy that only allows 100 concurrent connections at any one
//	// point.
//	maxConns := policy.Max(100)
//	// Create a policy that only allows 1 connection attempt per second per IP
//	// address.
//	rateLimit := policy.RateLimit(1.0, 1, 65535)
//	// Compose these policies together to require that all of them pass.
//	all := policy.All(maxConns, rateLimit)
//	// Or, compose these policies together to require that any of them pass.
//	any := policy.Any(maxConns, rateLimit)
//
// Timeout functions are similarly composable.
//
//	// Create a policy to Timeout after 1 second.
//	one := policy.ConstantTimeout(time.Second)
//	// Create a policy to scale this constant timeout by 60% with every attempt.
//	backoff := policy.LinearBackoff(1.6, one)
//	// Create a policy to clamp the Timeout to an upper bound of one minute, no
//	// matter how many attempts there have been.
//	max := policy.MaxTimeout(time.Minute, backoff)
//
// The policy functions available by default (of course, the programmer is free
// to implement their own policy functions) are quite simple. But, by providing
// "higher order policies" such as All, Any, or LinearBackoff, policies can be
// composed in more interesting ways. In practice, most of the policies that
// should be implemented at the raw connection level can be created by composing
// the available policies.
package policy
