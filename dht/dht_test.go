package dht_test

import (
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/renproject/aw/protocol"
	"github.com/renproject/aw/testutil"
)

var _ = Describe("DHT", func() {

	contain := func(addrs protocol.PeerAddresses, addr protocol.PeerAddress) bool {
		for _, address := range addrs {
			if address.Equal(addr) {
				return true
			}
		}
		return false
	}

	Context("when creating DHT", func() {
		Context("when initializing with a clean setup", func() {
			It("should has zero PeerAddress in its table", func() {
				addr := testutil.RandomAddress()
				dht := testutil.NewDHT(addr)

				Expect(dht.Me().Equal(addr)).Should(BeTrue())

				num, err := dht.NumPeers()
				Expect(err).ToNot(HaveOccurred())
				Expect(num).Should(BeZero())

				addrs, err := dht.PeerAddresses()
				Expect(err).ToNot(HaveOccurred())
				Expect(len(addrs)).Should(BeZero())
			})
		})

		Context("when initializing with some bootstrap nodes addresses", func() {
			It("should has have the bootstrap nodes inserted when i", func() {
				addr := testutil.RandomAddress()
				dht := testutil.NewDHT(addr)

				Expect(dht.Me().Equal(addr)).Should(BeTrue())

				num, err := dht.NumPeers()
				Expect(err).ToNot(HaveOccurred())
				Expect(num).Should(BeZero())

				addrs, err := dht.PeerAddresses()
				Expect(err).ToNot(HaveOccurred())
				Expect(len(addrs)).Should(BeZero())
			})
		})
	})

	Context("when adding a address to the dht", func() {
		It("should store the addresses in its table", func() {
			test := func() bool {
				dht := testutil.NewDHT(testutil.RandomAddress())

				for i := 0; i < 10; i++ {
					newAddr := testutil.RandomAddress()
					Expect(dht.AddPeerAddress(newAddr)).NotTo(HaveOccurred())

					queriedAddr, err := dht.PeerAddress(newAddr.PeerID())
					Expect(err).NotTo(HaveOccurred())
					Expect(queriedAddr.Equal(newAddr)).Should(BeTrue())

					num, err := dht.NumPeers()
					Expect(err).ToNot(HaveOccurred())
					Expect(num).Should(Equal(i + 1))

					addrs, err := dht.PeerAddresses()
					Expect(err).ToNot(HaveOccurred())

					Expect(contain(addrs, newAddr)).Should(BeTrue())
				}

				return true
			}

			Expect(quick.Check(test, nil)).Should(BeNil())
		})
	})

	Context("when adding multi addresses to the dht each time", func() {
		It("should store the addresses in its table", func() {
			test := func() bool {
				dht := testutil.NewDHT(testutil.RandomAddress())

				newAddrs := testutil.RandomAddresses()
				Expect(dht.AddPeerAddresses(newAddrs)).NotTo(HaveOccurred())

				num, err := dht.NumPeers()
				Expect(err).ToNot(HaveOccurred())
				Expect(num).Should(Equal(len(newAddrs)))

				addrs, err := dht.PeerAddresses()
				Expect(err).ToNot(HaveOccurred())

				for _, addr := range newAddrs {
					Expect(contain(addrs, addr)).Should(BeTrue())
				}

				return true
			}

			Expect(quick.Check(test, nil)).Should(BeNil())
		})
	})
})