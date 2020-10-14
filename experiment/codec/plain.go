package codec

import "io"

func PlainEncoder(w io.Writer, buf []byte) (int, error) {
	return w.Write(buf)
}

func PlainDecoder(r io.Reader, buf []byte) (int, error) {
	return io.ReadFull(r, buf)
}
