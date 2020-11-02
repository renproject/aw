package codec

import (
	"io"
)

func PlainEncoder(w io.Writer, buf []byte) (int, error) {
	return w.Write(buf)
}

func PlainDecoder(r io.Reader, buf []byte) (int, error) {
	n, err := io.ReadFull(r, buf)
	return n, err
}
