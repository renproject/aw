package wireutil_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestWireutil(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Wireutil Suite")
}
