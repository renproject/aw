package dhtutil

import (
	"math/rand"
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
