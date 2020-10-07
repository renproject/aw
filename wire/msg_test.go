package wire_test

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"testing/quick"
	"time"

	"github.com/renproject/aw/wire"
	"github.com/renproject/aw/wire/wireutil"
	"github.com/renproject/surge"
	"github.com/renproject/surge/surgeutil"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Message", func() {
	Context("when marshaling and unmarshaling", func() {
		It("should equal itself", func() {
			f := func() bool {
				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				msg := wireutil.NewMessageBuilder(r).Build()
				data, err := surge.ToBinary(msg)
				Expect(err).ToNot(HaveOccurred())
				unmarshaledMsg := wire.Message{}
				err = surge.FromBinary(&unmarshaledMsg, data)
				Expect(err).ToNot(HaveOccurred())
				Expect(msg.Equal(&unmarshaledMsg)).To(BeTrue())
				return true
			}
			Expect(quick.Check(f, &quick.Config{MaxCount: 10})).To(Succeed())
		})
	})

	Context("when sending and receiving valid messages", func() {
		It("should equal itself", func() {
			maxDataLen := 256
			rw := bytes.NewBuffer(make([]byte, maxDataLen+1+1+4)[:0])

			f := func() bool {
				rw.Reset()
				msg := wire.Message{
					Version: wire.Version(rand.Uint32()),
					Type:    wire.Type(rand.Uint32()),
					Data:    wire.Data(make([]byte, rand.Int()%maxDataLen)),
				}
				rand.Read(msg.Data[:])
				receivedMsg := wire.Message{}

				Expect(msg.Write(rw)).To(Succeed())
				Expect(receivedMsg.Read(rw)).To(Succeed())

				Expect(receivedMsg.Equal(&msg)).To(BeTrue())
				return true
			}
			Expect(quick.Check(f, nil)).To(Succeed())
		})
	})

	trials := 100
	types := []reflect.Type{
		reflect.TypeOf(wire.PingV1{}),
		reflect.TypeOf(wire.PingAckV1{}),
		reflect.TypeOf(wire.PushV1{}),
		reflect.TypeOf(wire.PushAckV1{}),
		reflect.TypeOf(wire.PullV1{}),
		reflect.TypeOf(wire.PullAckV1{}),
	}

	for _, t := range types {
		t := t

		Context(fmt.Sprintf("surge marshalling and unmarshalling for %v", t), func() {
			It("should be the same after marshalling and unmarshalling", func() {
				for i := 0; i < trials; i++ {
					Expect(surgeutil.MarshalUnmarshalCheck(t)).To(Succeed())
				}
			})

			It("should not panic when fuzzing", func() {
				for i := 0; i < trials; i++ {
					Expect(func() { surgeutil.Fuzz(t) }).ToNot(Panic())
				}
			})

			Context("marshalling", func() {
				It("should return an error when the buffer is too small", func() {
					for i := 0; i < trials; i++ {
						Expect(surgeutil.MarshalBufTooSmall(t)).To(Succeed())
					}
				})

				It("should return an error when the memory quota is too small", func() {
					for i := 0; i < trials; i++ {
						Expect(surgeutil.MarshalRemTooSmall(t)).To(Succeed())
					}
				})
			})

			Context("unmarshalling", func() {
				It("should return an error when the buffer is too small", func() {
					for i := 0; i < trials; i++ {
						Expect(surgeutil.UnmarshalBufTooSmall(t)).To(Succeed())
					}
				})

				It("should return an error when the memory quota is too small", func() {
					for i := 0; i < trials; i++ {
						Expect(surgeutil.UnmarshalRemTooSmall(t)).To(Succeed())
					}
				})
			})
		})
	}
})
