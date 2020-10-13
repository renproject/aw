package main

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"io"
	"log"
	"math/big"
	mathRand "math/rand"
	"net"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/crypto"

	"github.com/renproject/aw/conn"
	"github.com/renproject/aw/handshake2"
	"github.com/renproject/id"
	"github.com/renproject/pack"
	"github.com/renproject/surge"
)

func errHandler(err error) {
	log.Println(err)
}

func RunServer(address string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := conn.Listen(ctx, address, serverHandler(), errHandler, nil); err != nil {
		errHandler(err)
	}
}

func serverHandler() func(c net.Conn) {
	keyPair := id.NewPrivKey()

	return func(c net.Conn) {

	}
}

func serverHandlerHelper() {

}

func clientHandler() func(c net.Conn) {
	keyPair := id.NewPrivKey()
	return func(c net.Conn) {
		sPubKeyBuf := [64]byte{}
		_, err := io.ReadFull(c, sPubKeyBuf[:])

		xAsU256 := pack.NewU256FromInt(big.NewInt(0))
		yAsU256 := pack.NewU256FromInt(big.NewInt(0))
		if err := surge.FromBinary(&xAsU256, sPubKeyBuf[:32]); err != nil {

		}
		if err := surge.FromBinary(&yAsU256, sPubKeyBuf[32:]); err != nil {

		}

		sPubKey := ecdsa.PublicKey{
			Curve: crypto.S256(),
			X:     xAsU256.Int(),
			Y:     yAsU256.Int(),
		}

		r := mathRand.New(mathRand.NewSource(time.Now().UnixNano()))
		handshake := handshake2.ECIESClientHandshake(keyPair, (*id.PubKey)(&sPubKey), r)
		_, _, err = handshake(c, handshake2.PlainEncoder, handshake2.PlainDecoder)
		if err != nil {
			println("Success")
		}
	}
}

func RunClient(address string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := conn.Dial(ctx, address, clientHandler(), errHandler, nil); err != nil {
		errHandler(err)
	}
}

func main() {
	args := os.Args
	if len(args) < 3 {
		panic(fmt.Errorf("Insufficient arguments"))
	}

	switch args[1] {
	case "server":
		RunServer(args[2])
	case "client":
		RunClient(args[2])
	default:
		panic(fmt.Errorf("Invalid argument. Should specify server/client"))
	}
}
