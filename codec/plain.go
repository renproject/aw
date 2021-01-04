package codec

import (
	"io"
)

// PlainEncoder writes data directly to the IO writer without modification. The
// entire buffer is written.
func PlainEncoder(w io.Writer, buf []byte) (int, error) {
	return w.Write(buf)
}

// PlainDecoder reads data directly from the IO reader without modification. The
// entire buffer will be filled by reading data from the IO reader. This means
// that the buffer must be of the right length with respect to the data that is
// being read.
func PlainDecoder(r io.Reader, buf []byte) (int, error) {
	n, err := io.ReadFull(r, buf)
	return n, err
}
