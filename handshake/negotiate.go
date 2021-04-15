package handshake

import (
	"bytes"
	"fmt"
	"net"

	"github.com/renproject/aw/codec"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"github.com/renproject/surge"
)

const VERSION_BYTES = 2
type versionType = uint16

func Negotiate(self id.Signatory, h Handshake) Handshake {
	return func(conn net.Conn, enc codec.Encoder, dec codec.Decoder) (codec.Encoder, codec.Decoder, id.Signatory, error) {
		e, d, remote, err := h(conn, enc, dec)
		if err != nil {
			return nil, nil, remote, fmt.Errorf("handshake before negotiating version: %v", err)
		}

		var versionBytes [VERSION_BYTES]byte
		var version versionType
		cmp := bytes.Compare(self[:], remote[:])
		if cmp < 0 {
			if _, err := dec(conn, versionBytes[:]); err != nil {
				return nil, nil, remote, fmt.Errorf("decoding version: %v", err)
			}
			if _, _, err := surge.UnmarshalU16(&version, versionBytes[:], len(versionBytes)); err != nil {
				return nil, nil, remote, fmt.Errorf("unmarshaling version: %v", err)
			}
			if _, err := enc(conn, versionBytes[:]); err != nil {
				return nil, nil, remote, fmt.Errorf("encoding current version: %v", err)
			}
			if version == wire.CurrentVersion {
				return e, d, remote, nil
			}
			return nil, nil, remote, fmt.Errorf("not current version: %v", err)
		}
		if _, _, err := surge.MarshalU16(wire.CurrentVersion, versionBytes[:], len(versionBytes)); err != nil {
			return nil, nil, remote, fmt.Errorf("marshaling current version: %v", err)
		}
		if _, err := enc(conn, versionBytes[:]); err != nil {
			return nil, nil, remote, fmt.Errorf("encoding version: %v", err)
		}
		if _, err := dec(conn, versionBytes[:]); err != nil {
			return nil, nil, remote, fmt.Errorf("decoding version: %v", err)
		}
		if _, _, err := surge.UnmarshalU16(&version, versionBytes[:], len(versionBytes)); err != nil {
			return nil, nil, remote, fmt.Errorf("unmarshaling version: %v", err)
		}
		if version == wire.CurrentVersion {
			return e, d, remote, nil
		}
		return nil, nil, remote, fmt.Errorf("not current version: %v", err)
	}
}
