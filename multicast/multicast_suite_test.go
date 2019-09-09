package multicast_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestMulticast(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Multicast Suite")
}
