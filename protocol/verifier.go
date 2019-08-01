package protocol

type SignVerifier interface {
	Sign(digest []byte) ([]byte, error)
	Verify(digest, sig []byte) error
}
