package dhtutil

import (
	"crypto/rand"
	mrand "math/rand"
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
