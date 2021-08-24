package peer

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestPeer(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Peer Test Suite")
}
