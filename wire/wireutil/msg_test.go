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
		Context("when setting the version", func() {
			It("should build a message with that version", func() {
				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				f := func() bool {
					version := wireutil.RandomMessageVersion(r)
					msg := wireutil.NewMessageBuilder(r).WithVersion(version).Build()
					Expect(msg.Version).To(Equal(version))
					return true
				}
				Expect(quick.Check(f, &quick.Config{MaxCount: 10})).To(Succeed())
			})
		})

		Context("when setting the type", func() {
			It("should build a message with that type", func() {
				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				f := func() bool {
					ty := wireutil.RandomMessageType(r)
					msg := wireutil.NewMessageBuilder(r).WithType(ty).Build()
					Expect(msg.Type).To(Equal(ty))
					return true
				}
				Expect(quick.Check(f, &quick.Config{MaxCount: 10})).To(Succeed())
			})
		})

		Context("when setting the data", func() {
			It("should build a message with that data", func() {
				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				f := func() bool {
					data := wireutil.RandomMessageData(r)
					msg := wireutil.NewMessageBuilder(r).WithData(data).Build()
					Expect(bytes.Equal(msg.Data, data)).To(BeTrue())
					return true
				}
				Expect(quick.Check(f, &quick.Config{MaxCount: 10})).To(Succeed())
			})
		})
	})
})
