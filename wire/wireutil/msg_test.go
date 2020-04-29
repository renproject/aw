package wireutil_test

import (
	"bytes"
	"math/rand"
	"testing/quick"
	"time"

	"github.com/renproject/aw/wire/wireutil"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Message builder", func() {
	Context("when building messages", func() {
		Context("when settting the version", func() {
			It("should build a message with that version", func() {
				f := func() bool {
					r := rand.New(rand.NewSource(time.Now().UnixNano()))
					version := wireutil.RandomMessageVersion(r)
					msg := wireutil.NewMessageBuilder(r).WithVersion(version).Build()
					Expect(msg.Version).To(Equal(version))
					return true
				}
				Expect(quick.Check(f, nil)).To(Succeed())
			})
		})

		Context("when settting the type", func() {
			It("should build a message with that type", func() {
				f := func() bool {
					r := rand.New(rand.NewSource(time.Now().UnixNano()))
					ty := wireutil.RandomMessageType(r)
					msg := wireutil.NewMessageBuilder(r).WithType(ty).Build()
					Expect(msg.Type).To(Equal(ty))
					return true
				}
				Expect(quick.Check(f, nil)).To(Succeed())
			})
		})

		Context("when settting the data", func() {
			It("should build a message with that data", func() {
				f := func() bool {
					r := rand.New(rand.NewSource(time.Now().UnixNano()))
					data := wireutil.RandomMessageData(r)
					msg := wireutil.NewMessageBuilder(r).WithData(data).Build()
					Expect(bytes.Equal(msg.Data, data)).To(BeTrue())
					return true
				}
				Expect(quick.Check(f, nil)).To(Succeed())
			})
		})
	})
})
