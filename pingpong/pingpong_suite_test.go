package pingpong_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestPingpong(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pingpong Suite")
}
