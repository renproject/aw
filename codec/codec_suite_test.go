package codec_test

import (
"testing"

. "github.com/onsi/ginkgo"
. "github.com/onsi/gomega"
)

func TestCodec(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Codec Suite")
}