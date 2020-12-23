package aw_test

import (
	"testing"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestAwUpstream(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "AwUpstream Suite")
}
