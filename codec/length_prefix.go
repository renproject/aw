package codec

import (
	"encoding/binary"
	"fmt"
	"io"
)

func LengthPrefixEncoder(prefixEnc Encoder, bodyEnc Encoder) Encoder {
	return func(w io.Writer, buf []byte) (int, error) {
		prefix := uint32(len(buf))
		prefixBytes := [4]byte{}
		binary.BigEndian.PutUint32(prefixBytes[:], prefix)
		if _, err := prefixEnc(w, prefixBytes[:]); err != nil {
			return 0, fmt.Errorf("encoding data length: %v", err)
		}
		n, err := bodyEnc(w, buf)
		if err != nil {
			return n, fmt.Errorf("encoding data: %v", err)
		}
		return n, nil
	}
}

func LengthPrefixDecoder(prefixDec Decoder, bodyDec Decoder) Decoder {
	return func(r io.Reader, buf []byte) (int, error) {
		prefixBytes := [4]byte{}
		if _, err := prefixDec(r, prefixBytes[:]); err != nil {
			return 0, fmt.Errorf("decoding data length: %v", err)
		}
		prefix := binary.BigEndian.Uint32(prefixBytes[:])
		if uint32(len(buf)) < prefix {
			return 0, fmt.Errorf("decoding data length: expected %v, got %v", len(buf), prefix)
		}
		n, err := bodyDec(r, buf[:prefix])
		if err != nil {
			return n, fmt.Errorf("decoding data: %v", err)
		}
		return n, nil
	}
}
