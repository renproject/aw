package dht_test

import (
	"math/rand"
	"testing/quick"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/renproject/aw/dht"
	. "github.com/renproject/aw/testutil"
	"github.com/renproject/phi"
)

var _ = Describe("DHT", func() {

	Context("when creating DHT", func() {
		Context("when initializing with no bootstrap address", func() {
			It("should has zero PeerAddress in its table", func() {
				test := func() bool {
					me := RandomAddress()
					dht := NewDHT(me, NewTable("dht"), nil)

					Expect(dht.Me().Equal(me)).Should(BeTrue())

					num, err := dht.NumPeers()
					Expect(err).ToNot(HaveOccurred())
					Expect(num).Should(BeZero())

					addrs, err := dht.PeerAddresses()
					Expect(err).ToNot(HaveOccurred())
					return len(addrs) == 0
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when initializing with some bootstrap addresses", func() {
			It("should has have the bootstrap addresses inserted when initializing", func() {
				test := func() bool {
					me, bootstrapAddress := RandomAddress(), RandomAddresses()
					dht := NewDHT(me, NewTable("dht"), bootstrapAddress)
					Expect(dht.Me().Equal(me)).Should(BeTrue())

					// All bootstrap addresses should be queryable.
					for _, addr := range bootstrapAddress {
						stored, err := dht.PeerAddress(addr.PeerID())
						Expect(err).ToNot(HaveOccurred())
						Expect(stored.Equal(addr)).Should(BeTrue())
					}

					// All stored addresses are bootstrap addresses.
					num, err := dht.NumPeers()
					Expect(err).ToNot(HaveOccurred())
					Expect(num).Should(Equal(len(bootstrapAddress)))

					addrs, err := dht.PeerAddresses()
					Expect(err).ToNot(HaveOccurred())
					Expect(len(addrs)).Should(Equal(len(bootstrapAddress)))
					for _, addr := range addrs {
						Expect(Contains(bootstrapAddress, addr)).Should(BeTrue())
					}
					return true
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when initializing with a non-nil storage", func() {
			It("should load the address from store into cache for fast io", func() {
				test := func() bool {
					me, bootstrapAddress := RandomAddress(), RandomAddresses()
					store := NewTable("dht")
					_ = NewDHT(me, store, bootstrapAddress)

					dht := NewDHT(me, store, nil)

					// All stored addresses are bootstrap addresses.
					num, err := dht.NumPeers()
					Expect(err).ToNot(HaveOccurred())
					Expect(num).Should(Equal(len(bootstrapAddress)))

					addrs, err := dht.PeerAddresses()
					Expect(err).ToNot(HaveOccurred())
					Expect(len(addrs)).Should(Equal(len(bootstrapAddress)))
					for _, addr := range addrs {
						Expect(Contains(bootstrapAddress, addr)).Should(BeTrue())
					}
					return true
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})
	})

	Context("when adding, updating and deleting addresses", func() {
		It("should be able to add and delete new addresses to dht", func() {
			test := func() bool {
				me := RandomAddress()
				dht := NewDHT(me, NewTable("dht"), nil)

				newAddr := RandomAddress()
				queriedAddr, err := dht.PeerAddress(newAddr.PeerID())
				Expect(err).Should(HaveOccurred())

				Expect(dht.AddPeerAddress(newAddr)).NotTo(HaveOccurred())
				queriedAddr, err = dht.PeerAddress(newAddr.PeerID())
				Expect(err).NotTo(HaveOccurred())
				Expect(queriedAddr.Equal(newAddr)).Should(BeTrue())

				num, err := dht.NumPeers()
				Expect(err).ToNot(HaveOccurred())
				Expect(num).Should(Equal(1))

				addrs, err := dht.PeerAddresses()
				Expect(err).ToNot(HaveOccurred())
				Expect(Contains(addrs, newAddr)).Should(BeTrue())

				// Should be able to delete the ID from dht
				Expect(dht.RemovePeerAddress(newAddr.PeerID())).NotTo(HaveOccurred())

				num, err = dht.NumPeers()
				Expect(err).ToNot(HaveOccurred())
				Expect(num).Should(BeZero())

				addrs, err = dht.PeerAddresses()
				Expect(err).ToNot(HaveOccurred())
				return !Contains(addrs, newAddr)
			}

			Expect(quick.Check(test, nil)).Should(BeNil())
		})

		It("should be able to update a PeerAddress and return a boolean showing whether the address is newer", func() {
			test := func() bool {
				me := RandomAddress()
				dht := NewDHT(me, NewTable("dht"), nil)

				newAddr := RandomAddress()
				Expect(dht.AddPeerAddress(newAddr)).NotTo(HaveOccurred())

				ok, err := dht.UpdatePeerAddress(newAddr)
				Expect(err).NotTo(HaveOccurred())
				Expect(ok).Should(BeFalse())

				// Update the nonce
				newAddr.Nonce = time.Now().Unix()

				ok, err = dht.UpdatePeerAddress(newAddr)
				Expect(err).NotTo(HaveOccurred())
				Expect(ok).Should(BeTrue())
				return ok
			}

			Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
		})

		Context("when calling different functions concurrently", func() {
			It("should be concurrent safe to use", func() {
				addAndDelete := func(dht dht.DHT) error {
					newAddr := RandomAddress()
					if err := dht.AddPeerAddress(newAddr); err != nil {
						return err
					}
					time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
					return dht.RemovePeerAddress(newAddr.PeerID())
				}

				update := func(dht dht.DHT) error {
					newAddr := RandomAddress()
					if err := dht.AddPeerAddress(newAddr); err != nil {
						return err
					}
					time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
					newAddr.Nonce = time.Now().Unix()
					if _, err := dht.UpdatePeerAddress(newAddr); err != nil {
						return err
					}

					return dht.RemovePeerAddress(newAddr.PeerID())
				}

				test := func() bool {
					me := RandomAddress()
					dht := NewDHT(me, NewTable("dht"), nil)
					errs := make([]error, 100)
					phi.ParForAll(100, func(i int) {
						if i < 50 {
							errs[i] = addAndDelete(dht)
						} else {
							errs[i] = update(dht)
						}
					})
					for _, err := range errs {
						Expect(err).Should(BeNil())
					}

					return true
				}
				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})
	})

	Context("when creating, querying and deleting PeerGroups", func() {
		It("should be able to adding new group and delete a existing group", func() {
			test := func() bool {
				me := RandomAddress()
				dht := NewDHT(me, NewTable("dht"), nil)

				groupID, ids := NewPeerGroupID(), RandomPeerIDs()
				dht.NewPeerGroup(NewPeerGroupID(), ids)

				addrs, ok := dht.PeerGroup(groupID)
				Expect(ok).Should(BeTrue())
				Expect(len(addrs)).Should(Equal(len(ids)))

				dht.RemovePeerGroup(groupID)

				addrs, ok = dht.PeerGroup(groupID)
				Expect(ok).Should(BeFalse())
				Expect(addrs).Should(BeNil())
				return true
			}

			Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
		})
	})
})
