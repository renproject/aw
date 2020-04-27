package gossip_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestGossip(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Gossip Suite")
}
