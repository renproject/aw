package dht_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDHT(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "DHT Suite")
}
