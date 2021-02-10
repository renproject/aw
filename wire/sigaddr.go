package wire

import (
	"fmt"
	"github.com/renproject/id"
)

type SignatoryAndAddress struct {
	Signatory id.Signatory
	Address   Address
}

func (sigAndAddr SignatoryAndAddress) Marshal(buf []byte, rem int) ([]byte, int, error) {
	buf, rem, err := sigAndAddr.Signatory.Marshal(buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("marshal signatory: %v", err)
	}
	buf, rem, err = sigAndAddr.Address.Marshal(buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("marshal address: %v", err)
	}
	return buf, rem, err
}
func (sigAndAddr *SignatoryAndAddress) Unmarshal(buf []byte, rem int) ([]byte, int, error) {
	buf, rem, err := (&sigAndAddr.Signatory).Unmarshal(buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("unmarshal signatory: %v", err)
	}
	buf, rem, err = sigAndAddr.Address.Unmarshal(buf, rem)
	if err != nil {
		return buf, rem, fmt.Errorf("unmarshal address: %v", err)
	}
	return buf, rem, err
}
