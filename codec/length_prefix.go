package codec

import (
	"encoding/binary"
	"fmt"
	"io"
)

// LengthPrefixEncoder returns an Encoder that prefixes all data with a uint32
// length. The returned Encoder wraps two other Encoders, one that is used to
// encode the length prefix, and one that is used to encode the actual data.
func LengthPrefixEncoder(prefixEnc Encoder, bodyEnc Encoder) Encoder {
	return func(w io.Writer, buf []byte) (int, error) {
		prefix := uint32(len(buf))
		prefixBytes := [4]byte{}
		binary.BigEndian.PutUint32(prefixBytes[:], prefix)
		if _, err := prefixEnc(w, prefixBytes[:]); err != nil {
			return 0, fmt.Errorf("encoding data length: %w", err)
		}
		n, err := bodyEnc(w, buf)
		if err != nil {
			return n, fmt.Errorf("encoding data: %w", err)
		}
		return n, nil
	}
}

// LengthPrefixDecoder returns an Decoder that assumes all data is prefixed with
// a uint32 length. The returned Decoder wraps two other Decoders, one that is
// used to decode the length prefix, and one that is used to decode the actual
// data.
func LengthPrefixDecoder(prefixDec Decoder, bodyDec Decoder) Decoder {
	return func(r io.Reader, buf []byte) (int, error) {
		prefixBytes := [4]byte{}
		if _, err := prefixDec(r, prefixBytes[:]); err != nil {
			return 0, fmt.Errorf("decoding data length: %w", err)
		}
		prefix := binary.BigEndian.Uint32(prefixBytes[:])
		if uint32(len(buf)) < prefix {
			return 0, fmt.Errorf("decoding data length: expected %v, got %v", len(buf), prefix)
		}
		n, err := bodyDec(r, buf[:prefix])
		if err != nil {
			return n, fmt.Errorf("decoding data: %w", err)
		}
		return n, nil
	}
}
