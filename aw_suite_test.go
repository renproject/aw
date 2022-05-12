package aw_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestAirwave(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Airwave Suite")
}
