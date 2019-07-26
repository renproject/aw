package handshake

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"golang.org/x/crypto/sha3"

	"github.com/renproject/aw/protocol"
)

type HandShaker interface {
	SendHandShakeMessage(ctx context.Context, rw io.ReadWriter, myAddress []byte) error
	ValidateHandShakeMessage(ctx context.Context, rw io.ReadWriter, remoteAddress []byte) error
}

type handShaker struct {
	signVerifier protocol.SignVerifier
}

func NewHandShaker(signVerifier protocol.SignVerifier) HandShaker {
	return &handShaker{signVerifier: signVerifier}
}

func (hs *handShaker) SendHandShakeMessage(ctx context.Context, rw io.ReadWriter, myAddress []byte) error {
	timeStamp := [8]byte{}
	_ = binary.PutVarint(timeStamp[:], time.Now().Unix())
	data := append(timeStamp[:], myAddress...)
	hash := sha3.Sum256(data)
	sig, err := hs.signVerifier.Sign(hash[:])
	if err != nil {
		return err
	}
	if err := binary.Write(rw, binary.LittleEndian, uint64(len(data)+65)); err != nil {
		return err
	}
	if _, err := rw.Write(append(data, sig...)); err != nil {
		return err
	}
	return nil
}

func (hs *handShaker) ValidateHandShakeMessage(ctx context.Context, rw io.ReadWriter, remoteAddress []byte) error {
	var msgLen uint64
	if err := binary.Read(rw, binary.LittleEndian, &msgLen); err != nil {
		return err
	}
	msg := make([]byte, msgLen)
	if _, err := rw.Read(msg); err != nil {
		return err
	}
	data := msg[:msgLen-65]
	sig := msg[msgLen-65:]
	hash := sha3.Sum256(data)
	if err := hs.signVerifier.Verify(hash[:], sig); err != nil {
		return err
	}
	timeStamp, err := binary.ReadVarint(bytes.NewBuffer(data[:8]))
	if err != nil {
		return err
	}
	if time.Since(time.Unix(timeStamp, 0)) > time.Minute {
		return fmt.Errorf("signature expired")
	}
	if bytes.Compare(data[8:], remoteAddress) != 0 {
		return fmt.Errorf("possible mim attack")
	}
	return nil
}
