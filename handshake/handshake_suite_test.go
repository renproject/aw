package handshake_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestTCP(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Handshake suite")
}
