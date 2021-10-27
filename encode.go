package aw

import (
	"encoding/binary"
	"fmt"

	"github.com/renproject/aw/session"
)

func encode(src, dst []byte, session *session.GCMSession) []byte {
	nonceBuf := session.GetWriteNonceAndIncrement()

	// TODO(ross): It seems that you can give the plaintext buffer as the first
	// input to reuse the memory, consider doing this.
	sealed := session.GCM.Seal(dst[4:4], nonceBuf[:], src, nil)

	binary.BigEndian.PutUint32(dst, uint32(len(sealed)))
	return dst[:4+len(sealed)]
}

func decode(src, dst []byte, session *session.GCMSession) ([]byte, error) {
	nonceBuf := session.GetReadNonceAndIncrement()
	decrypted, err := session.GCM.Open(dst[:0], nonceBuf[:], src, nil)
	if err != nil {
		return dst[:0], fmt.Errorf("opening sealed data: %v", err)
	}

	return decrypted, nil
}
