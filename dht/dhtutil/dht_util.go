package dhtutil

import (
	"github.com/renproject/id"
	"math/rand"
	"reflect"
	"sort"
	"time"
)

var defaultContent = [5]byte{10, 20, 30, 40, 50}

func RandomContent() []byte {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	length := rand.Intn(20) + 10
	content := make([]byte, length)

	_, err := r.Read(content)
	if err != nil {
		return defaultContent[:]
	}
	return content
}

// IsSorted checks if the given list of addresses are sorted in order of their
// XOR didstance from our own address.
func IsSorted(identity id.Signatory, addrs []id.Signatory) bool {
	sortedAddrs := make([]id.Signatory, len(addrs))
	copy(sortedAddrs, addrs)
	SortAddrs(identity, sortedAddrs)
	return reflect.DeepEqual(addrs, sortedAddrs)
}

// SortAddrs in order of their XOR distance from our own address.
func SortAddrs(identity id.Signatory, addrs []id.Signatory) {
	sort.Slice(addrs, func(i, j int) bool {
		fstSignatory := addrs[i]
		sndSignatory := addrs[j]
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
