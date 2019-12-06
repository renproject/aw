package dht_test

import (
	"math/rand"
	"testing/quick"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/renproject/aw/dht"
	. "github.com/renproject/aw/testutil"

	"github.com/renproject/aw/protocol"
	"github.com/renproject/phi"
)

var _ = Describe("DHT", func() {

	Context("when creating DHT", func() {
		Context("when passing nil parameters", func() {
			It("should panic if provide nil self address or nil codec", func() {
				Expect(func() {
					New(nil, NewSimpleTCPPeerAddressCodec(), NewTable("dht"))
				}).Should(Panic())

				Expect(func() {
					New(RandomAddress(), nil, NewTable("dht"))
				}).Should(Panic())
			})
		})

		Context("when initializing with no bootstrap address", func() {
			It("should has zero PeerAddress in its table", func() {
				test := func() bool {
					me := RandomAddress()
					dht := NewDHT(me, nil, nil)

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
					me, bootstrapAddress := RandomAddress(), RandomAddresses(rand.Intn(32))
					dht := NewDHT(me, NewTable("dht"), bootstrapAddress)
					Expect(dht.Me().Equal(me)).Should(BeTrue())

					// All bootstrap addresses should be queryable.
					for _, addr := range bootstrapAddress {
						if addr.PeerID().Equal(me.ID) {
							continue
						}
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
						Expect(bootstrapAddress).Should(ContainElement(addr))
					}
					return true
				}

				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when initializing with a non-nil storage", func() {
			It("should load the address from store into cache for fast io", func() {
				test := func() bool {
					me, bootstrapAddress := RandomAddress(), RandomAddresses(rand.Intn(32)+1)
					for ContainAddress(bootstrapAddress, me){
						me = RandomAddress()
					}
					store := NewTable("dht")
					_ = NewDHT(me, store, bootstrapAddress[:len(bootstrapAddress)-1])

					dht := NewDHT(me, store, bootstrapAddress)

					// All stored addresses are bootstrap addresses.
					num, err := dht.NumPeers()
					Expect(err).ToNot(HaveOccurred())
					Expect(num).Should(Equal(len(bootstrapAddress)))

					addrs, err := dht.PeerAddresses()
					Expect(err).ToNot(HaveOccurred())
					Expect(len(addrs)).Should(Equal(len(bootstrapAddress)))
					for _, addr := range addrs {
						Expect(bootstrapAddress).Should(ContainElement(addr))
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
				Expect(addrs).Should(ContainElement(newAddr))

				// Should be able to delete the ID from dht
				Expect(dht.RemovePeerAddress(newAddr.PeerID())).NotTo(HaveOccurred())

				num, err = dht.NumPeers()
				Expect(err).ToNot(HaveOccurred())
				Expect(num).Should(BeZero())

				addrs, err = dht.PeerAddresses()
				Expect(err).ToNot(HaveOccurred())

				Expect(addrs).ToNot(ContainElement(newAddr))
				return true
			}

			Expect(quick.Check(test, nil)).Should(BeNil())
		})

		It("should be able to update a PeerAddress and return a boolean showing whether the address is newer", func() {
			test := func() bool {
				dht := NewDHT(RandomAddress(), NewTable("dht"), nil)

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
				addAndDelete := func(dht DHT) error {
					newAddr := RandomAddress()
					if err := dht.AddPeerAddress(newAddr); err != nil {
						return err
					}
					time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
					return dht.RemovePeerAddress(newAddr.PeerID())
				}

				update := func(dht DHT) error {
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
				dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
				groupID, peerAddrs := RandomPeerGroupID(), RandomAddresses(rand.Intn(32))
				for _, peerAddr := range peerAddrs {
					Expect(dht.AddPeerAddress(peerAddr)).NotTo(HaveOccurred())
				}
				peerIDs := FromAddressesToIDs(peerAddrs)

				// Before adding the peer group
				ids, err := dht.PeerGroupIDs(groupID)
				Expect(err).To(HaveOccurred())
				_, err = dht.PeerGroupAddresses(groupID)
				Expect(err).To(HaveOccurred())

				// Adding the peer group
				Expect(dht.AddPeerGroup(groupID, peerIDs)).NotTo(HaveOccurred())
				ids, err = dht.PeerGroupIDs(groupID)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(ids)).Should(Equal(len(peerIDs)))
				addrs, err := dht.PeerGroupAddresses(groupID)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(addrs)).Should(Equal(len(peerIDs)))

				// After removing the peer group
				dht.RemovePeerGroup(groupID)
				ids, err = dht.PeerGroupIDs(groupID)
				Expect(err).To(HaveOccurred())
				_, err = dht.PeerGroupAddresses(groupID)
				Expect(err).To(HaveOccurred())
				return true
			}

			Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
		})

		It("should return an error when trying to add a group with nil id", func() {
			test := func() bool {
				dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
				Expect(dht.AddPeerGroup(protocol.NilPeerGroupID, RandomPeerIDs())).To(HaveOccurred())
				return true
			}

			Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
		})

		It("should be able to return all peers when providing a nil group id", func() {
			test := func() bool {
				dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
				peerAddrs := RandomAddresses(rand.Intn(32))
				ids := FromAddressesToIDs(peerAddrs)
				for i := range peerAddrs {
					Expect(dht.AddPeerAddress(peerAddrs[i])).NotTo(HaveOccurred())
				}

				storedIDs, err := dht.PeerGroupIDs(protocol.NilPeerGroupID)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(ids)).Should(Equal(len(storedIDs)))
				for _, id := range storedIDs {
					Expect(ids).Should(ContainElement(id))
				}

				storedAddrs, err := dht.PeerGroupAddresses(protocol.NilPeerGroupID)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(peerAddrs)).Should(Equal(len(storedAddrs)))
				for _, addr := range storedAddrs {
					Expect(peerAddrs).Should(ContainElement(addr))
				}
				return true
			}

			Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
		})

		It("should only return the PeerAddresses we have when querying with a groupID", func() {
			test := func() bool {
				dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
				peerAddrs := RandomAddresses(rand.Intn(32) + 1)
				ids := FromAddressesToIDs(peerAddrs)

				// Purposely not adding the last PeerAddress
				for i := range peerAddrs[:len(peerAddrs)-1] {
					Expect(dht.AddPeerAddress(peerAddrs[i])).NotTo(HaveOccurred())
				}
				groupID := RandomPeerGroupID()
				Expect(dht.AddPeerGroup(groupID, ids)).NotTo(HaveOccurred())

				storedIDs, err := dht.PeerGroupIDs(groupID)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(ids)).Should(Equal(len(storedIDs)))
				for _, id := range storedIDs {
					Expect(ids).Should(ContainElement(id))
				}

				storedAddrs, err := dht.PeerGroupAddresses(groupID)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(storedAddrs)).Should(Equal(len(peerAddrs) - 1))
				for _, addr := range storedAddrs {
					Expect(peerAddrs).Should(ContainElement(addr))
				}
				return true
			}

			Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
		})

		It("should be concurrent safe to use PeerGroup", func() {
			test := func() bool {
				dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
				groupID, peerAddrs := RandomPeerGroupID(), RandomAddresses(rand.Intn(32))
				peerIDs := FromAddressesToIDs(peerAddrs)
				Expect(dht.AddPeerGroup(groupID, peerIDs)).NotTo(HaveOccurred())

				// Try to add and query PeerGroups at the same time
				var err1, err2 error
				phi.ParBegin(func() {
					for _, peerAddr := range peerAddrs {
						if err1 = dht.AddPeerAddress(peerAddr); err1 != nil {
							return
						}
					}
				}, func() {
					_, err2 = dht.PeerGroupAddresses(groupID)
				})
				Expect(err1).NotTo(HaveOccurred())
				Expect(err2).NotTo(HaveOccurred())

				return true
			}

			Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
		})
	})

	Context("when retrieving random addresses from the dht", func() {
		Context("when not specifying a group id", func() {
			It("should be able to return specific number of random address in the dht", func() {
				test := func() bool {
					dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
					addrs := RandomAddresses(32)
					for i := range addrs {
						Expect(dht.AddPeerAddress(addrs[i])).NotTo(HaveOccurred())
					}

					n := rand.Intn(len(addrs)) + 1
					randAddrs, err := dht.RandomPeerAddresses(protocol.NilPeerGroupID, n)
					Expect(err).NotTo(HaveOccurred())
					Expect(len(randAddrs)).Should(Equal(n))
					for _, addr := range randAddrs {
						Expect(addrs).Should(ContainElement(addr))
					}

					randAddrs, err = dht.RandomPeerAddresses(protocol.NilPeerGroupID, len(addrs)+1+rand.Int())
					Expect(err).NotTo(HaveOccurred())
					Expect(len(randAddrs)).Should(Equal(len(addrs)))
					for _, addr := range randAddrs {
						Expect(addrs).Should(ContainElement(addr))
					}
					return true
				}
				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})

		Context("when specifying a group id", func() {
			It("should only return random peers in the given group", func() {
				test := func() bool {
					dht := NewDHT(RandomAddress(), NewTable("dht"), nil)
					addrs := RandomAddresses(64)
					for i := range addrs {
						Expect(dht.AddPeerAddress(addrs[i])).NotTo(HaveOccurred())
					}

					ids := FromAddressesToIDs(addrs)
					groupID1, groupID2 := RandomPeerGroupID(), RandomPeerGroupID()
					Expect(dht.AddPeerGroup(groupID1, ids[:32])).NotTo(HaveOccurred())
					Expect(dht.AddPeerGroup(groupID2, ids[32:])).NotTo(HaveOccurred())

					randAddrs, err := dht.RandomPeerAddresses(groupID1, 64)
					Expect(err).NotTo(HaveOccurred())
					Expect(len(randAddrs)).Should(Equal(32))
					for _, addr := range randAddrs {
						Expect(ids[:32]).Should(ContainElement(addr.PeerID()))
						Expect(ids[32:]).ShouldNot(ContainElement(addr.PeerID()))
					}

					randAddrs, err = dht.RandomPeerAddresses(groupID2, 64)
					Expect(err).NotTo(HaveOccurred())
					Expect(len(randAddrs)).Should(Equal(32))
					for _, addr := range randAddrs {
						Expect(ids[:32]).ShouldNot(ContainElement(addr.PeerID()))
						Expect(ids[32:]).Should(ContainElement(addr.PeerID()))
					}
					return true
				}
				Expect(quick.Check(test, nil)).NotTo(HaveOccurred())
			})
		})
	})
})
