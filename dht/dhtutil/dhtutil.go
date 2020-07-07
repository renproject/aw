package dhtutil

import (
	"crypto/rand"
	mrand "math/rand"
	"sort"

	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
)

// RandomContent returns a random byte slice.
func RandomContent() []byte {
	length := mrand.Intn(32) + 1
	content := make([]byte, length)
	_, err := rand.Read(content)
	if err != nil {
		panic(err)
	}
	return content
}

// SortAddrs in order of their XOR distance from our own address.
func SortAddrs(identity id.Signatory, addrs []wire.Address) {
	sort.Slice(addrs, func(i, j int) bool {
		fstSignatory, err := addrs[i].Signatory()
		if err != nil {
			return false
		}

		sndSignatory, err := addrs[j].Signatory()
		if err != nil {
			return false
		}

		return isCloser(identity, fstSignatory, sndSignatory)
	})
}

// SortSignatories in order of their XOR distance from our own address.
func SortSignatories(identity id.Signatory, signatories []id.Signatory) {
	sort.Slice(signatories, func(i, j int) bool {
		return isCloser(identity, signatories[i], signatories[j])
	})
}

func isCloser(identity, fst, snd id.Signatory) bool {
	for b := 0; b < 32; b++ {
		d1 := identity[b] ^ fst[b]
		d2 := identity[b] ^ snd[b]
		if d1 < d2 {
			return true
		}
		if d2 < d1 {
			return false
		}
	}
	return false
}
