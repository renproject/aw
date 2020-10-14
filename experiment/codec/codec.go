package codec

import (
	"io"
)

// An Encoder is a function that encodes a byte slice into an I/O writer. It
// returns the number of bytes written, and errors that happen.
type Encoder func(w io.Writer, buf []byte) (int, error)

// A Decoder is a function the decodes bytes from an I/O reader into a byte
// slice. It returns the number of bytes read, and errors that happen.
type Decoder func(r io.Reader, buf []byte) (int, error)
