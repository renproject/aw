package dhtutil_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestDhtutil(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "DHTutil Suite")
}
