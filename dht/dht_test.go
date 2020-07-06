package dht_test

import (
	"crypto/sha256"
	"math/rand"
	"sort"
	"testing/quick"
	"time"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/dht/dhtutil"
	"github.com/renproject/aw/wire"
	"github.com/renproject/aw/wire/wireutil"
	"github.com/renproject/id"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("DHT", func() {
	Describe("Addresses", func() {
		Context("when inserting an address", func() {
			It("should be able to query it", func() {
				table, _ := initDHT()

				f := func(seed int64) bool {
					privKey := id.NewPrivKey()
					addr := wireutil.NewAddressBuilder(
						privKey,
						rand.New(rand.NewSource(seed)),
					).Build()

					ok := table.InsertAddr(addr)
					Expect(ok).To(BeTrue())

					signatory := id.NewSignatory(&privKey.PublicKey)
					newAddr, ok := table.Addr(signatory)
					Expect(ok).To(BeTrue())
					Expect(newAddr).To(Equal(addr))
					return true
				}
				Expect(quick.Check(f, nil)).To(Succeed())
			})

			Context("if the address is new", func() {
				It("should return true", func() {
					table, _ := initDHT()

					f := func(seed int64) bool {
						addr := wireutil.NewAddressBuilder(
							id.NewPrivKey(),
							rand.New(rand.NewSource(seed)),
						).Build()

						return table.InsertAddr(addr)
					}
					Expect(quick.Check(f, nil)).To(Succeed())
				})
			})

			Context("if the address already exists", func() {
				It("should return false", func() {
					table, _ := initDHT()

					f := func(seed int64) bool {
						addr := wireutil.NewAddressBuilder(
							id.NewPrivKey(),
							rand.New(rand.NewSource(seed)),
						).Build()

						ok := table.InsertAddr(addr)
						Expect(ok).To(BeTrue())

						return !table.InsertAddr(addr)
					}
					Expect(quick.Check(f, nil)).To(Succeed())
				})
			})

			Context("if the address is old", func() {
				It("should return false", func() {
					table, _ := initDHT()

					f := func(seed int64) bool {
						privKey := id.NewPrivKey()
						addr := wireutil.NewAddressBuilder(
							privKey,
							rand.New(rand.NewSource(seed)),
						).Build()

						ok := table.InsertAddr(addr)
						Expect(ok).To(BeTrue())

						// Decrement the nonce so it is older and re-sign the
						// address.
						addr.Nonce -= 1
						err := addr.Sign(privKey)
						Expect(err).ToNot(HaveOccurred())

						return !table.InsertAddr(addr)
					}
					Expect(quick.Check(f, nil)).To(Succeed())
				})
			})
		})

		Context("when deleting an address", func() {
			It("should not be able to query it", func() {
				table, _ := initDHT()

				f := func(seed int64) bool {
					privKey := id.NewPrivKey()
					addr := wireutil.NewAddressBuilder(
						privKey,
						rand.New(rand.NewSource(seed)),
					).Build()

					// Try to delete the address prior to inserting to make sure
					// it does not panic.
					signatory := id.NewSignatory(&privKey.PublicKey)
					table.DeleteAddr(signatory)

					// Insert the address.
					ok := table.InsertAddr(addr)
					Expect(ok).To(BeTrue())

					// Delete the address and make sure it no longer exists when
					// querying the DHT.
					table.DeleteAddr(signatory)

					_, ok = table.Addr(signatory)
					return !ok
				}
				Expect(quick.Check(f, nil)).To(Succeed())
			})
		})

		Context("when querying addresses", func() {
			It("should return them in order of their XOR distance", func() {
				table, identity := initDHT()
				numAddrs := rand.Intn(990) + 10 // [10, 1000)

				// Insert `numAddrs` random addresses into the store.
				addrs := make([]wire.Address, numAddrs)
				for i := 0; i < numAddrs; i++ {
					privKey := id.NewPrivKey()
					addr := wireutil.NewAddressBuilder(
						privKey,
						rand.New(rand.NewSource(GinkgoRandomSeed()+1)),
					).Build()

					ok := table.InsertAddr(addr)
					Expect(ok).To(BeTrue())

					addrs = append(addrs, addr)
				}

				// Check addresses are returned in order of their XOR distance
				// from our own address.
				numNewAddrs := rand.Intn(numAddrs)
				newAddrs := table.Addrs(numNewAddrs)
				Expect(len(newAddrs)).To(Equal(numNewAddrs))

				sortedAddrs := make([]wire.Address, len(newAddrs))
				copy(sortedAddrs, newAddrs)
				sortAddrs(identity, sortedAddrs)
				Expect(newAddrs).To(Equal(sortedAddrs))
			})

			Context("if there are less than n addresses in the store", func() {
				It("should return all the addresses", func() {
					table, _ := initDHT()
					numAddrs := rand.Intn(100)

					// Insert `numAddrs` random addresses into the store.
					for i := 0; i < numAddrs; i++ {
						privKey := id.NewPrivKey()
						addr := wireutil.NewAddressBuilder(
							privKey,
							rand.New(rand.NewSource(GinkgoRandomSeed()+1)),
						).Build()

						ok := table.InsertAddr(addr)
						Expect(ok).To(BeTrue())
					}

					addrs := table.Addrs(100)
					Expect(len(addrs)).To(Equal(numAddrs))
				})
			})

			Context("if there are no addresses in the store", func() {
				It("should return no addresses", func() {
					table, _ := initDHT()

					addrs := table.Addrs(100)
					Expect(len(addrs)).To(Equal(0))

					addrs = table.Addrs(0)
					Expect(len(addrs)).To(Equal(0))
				})
			})
		})

		Context("when querying the number of addresses", func() {
			It("should return the correct amount", func() {
				table, _ := initDHT()
				numAddrs := rand.Intn(100)

				// Insert `numAddrs` random addresses into the store.
				for i := 0; i < numAddrs; i++ {
					privKey := id.NewPrivKey()
					addr := wireutil.NewAddressBuilder(
						privKey,
						rand.New(rand.NewSource(GinkgoRandomSeed()+1)),
					).Build()

					ok := table.InsertAddr(addr)
					Expect(ok).To(BeTrue())
				}

				n, err := table.NumAddrs()
				Expect(err).ToNot(HaveOccurred())
				Expect(n).To(Equal(numAddrs))
			})
		})
	})

	Describe("Content", func() {
		Context("when initialising a DHT without a content resolver", func() {
			It("should panic", func() {
				privKey := id.NewPrivKey()
				identity := id.NewSignatory(&privKey.PublicKey)
				Expect(func() { dht.New(identity, nil) }).To(Panic())
			})
		})

		Context("when inserting/deleting/querying content", func() {
			It("should use the content resolver", func() {
				insertCh := make(chan id.Hash)
				deleteCh := make(chan id.Hash)
				contentCh := make(chan id.Hash)

				privKey := id.NewPrivKey()
				identity := id.NewSignatory(&privKey.PublicKey)
				resolver := dhtutil.NewChannelResolver(insertCh, deleteCh, contentCh)
				table := dht.New(identity, resolver)

				// Insert and wait on the channel to make sure the inner
				// resolver received the message.
				hash := id.Hash(sha256.Sum256(dhtutil.RandomContent()))
				contentType := uint8(0)
				go table.InsertContent(hash, contentType, nil)

				newHash := <-insertCh
				Expect(newHash).To(Equal(hash))

				// Delete and wait on the channel to make sure the inner
				// resolver received the message.
				hash = id.Hash(sha256.Sum256(dhtutil.RandomContent()))
				go table.DeleteContent(hash, contentType)

				newHash = <-deleteCh
				Expect(newHash).To(Equal(hash))

				// Get and wait on the channel to make sure the inner resolver
				// received the message.
				hash = id.Hash(sha256.Sum256(dhtutil.RandomContent()))
				go table.Content(hash, contentType)

				newHash = <-contentCh
				Expect(newHash).To(Equal(hash))

				// Ensure the channels receive no additional messages.
				select {
				case <-insertCh:
					Fail("unexpected insert message")
				case <-deleteCh:
					Fail("unexpected delete message")
				case <-contentCh:
					Fail("unexpected content message")
				case <-time.After(time.Second):
				}
			})
		})

		Context("when checking if the DHT has content with a given hash", func() {
			Context("if the content exists", func() {
				It("should return true", func() {
					table, _ := initDHT()

					f := func(hash id.Hash, contentType uint8, content []byte) bool {
						table.InsertContent(hash, contentType, content)
						return table.HasContent(hash, contentType)
					}
					Expect(quick.Check(f, nil)).To(Succeed())
				})
			})

			Context("if the content does not exist", func() {
				It("should return false", func() {
					table, _ := initDHT()

					f := func(hash id.Hash, contentType uint8) bool {
						return !table.HasContent(hash, contentType)
					}
					Expect(quick.Check(f, nil)).To(Succeed())
				})
			})
		})

		Context("when checking if the DHT has empty content with a given hash", func() {
			Context("if the content exists and is empty", func() {
				It("should return true", func() {
					table, _ := initDHT()

					f := func(hash id.Hash, contentType uint8) bool {
						table.InsertContent(hash, contentType, nil)
						return table.HasEmptyContent(hash, contentType)
					}
					Expect(quick.Check(f, nil)).To(Succeed())
				})
			})

			Context("if the content exists and is not empty", func() {
				It("should return false", func() {
					table, _ := initDHT()

					f := func(hash id.Hash, contentType uint8, content []byte) bool {
						// If the random content is empty, return true.
						if len(content) == 0 {
							return true
						}
						table.InsertContent(hash, contentType, content)
						return !table.HasEmptyContent(hash, contentType)
					}
					Expect(quick.Check(f, nil)).To(Succeed())
				})
			})

			Context("if the content does not exist", func() {
				It("should return false", func() {
					table, _ := initDHT()

					f := func(hash id.Hash, contentType uint8) bool {
						return !table.HasEmptyContent(hash, contentType)
					}
					Expect(quick.Check(f, nil)).To(Succeed())
				})
			})
		})
	})

	Describe("Subnets", func() {
		Context("when adding a subnet", func() {
			It("should be able to query it", func() {
				table, identity := initDHT()

				// Generate a random number of signatories.
				numSignatories := rand.Intn(100)
				signatories := make([]id.Signatory, numSignatories)
				for i := 0; i < numSignatories; i++ {
					privKey := id.NewPrivKey()
					signatories[i] = id.NewSignatory(&privKey.PublicKey)
				}

				hash := table.AddSubnet(signatories)
				newSignatories := table.Subnet(hash)

				// Sort the original slice by XOR distance from our address and
				// verify it is equal to the result.
				sortSignatories(identity, signatories)
				Expect(newSignatories).To(Equal(signatories))
			})
		})

		Context("when deleting a subnet", func() {
			It("should not be able to query it", func() {
				table, _ := initDHT()

				// Generate a random number of signatories.
				numSignatories := rand.Intn(100)
				signatories := make([]id.Signatory, numSignatories)
				for i := 0; i < numSignatories; i++ {
					privKey := id.NewPrivKey()
					signatories[i] = id.NewSignatory(&privKey.PublicKey)
				}

				hash := table.AddSubnet(signatories)
				table.DeleteSubnet(hash)

				newSignatories := table.Subnet(hash)
				Expect(len(newSignatories)).To(Equal(0))
			})
		})

		Context("when querying a subnet that does not exist", func() {
			It("should return an empty list", func() {
				table, _ := initDHT()

				data := make([]byte, 32)
				_, err := rand.Read(data[:])
				Expect(err).ToNot(HaveOccurred())

				hash := id.NewHash(data)
				signatories := table.Subnet(hash)
				Expect(len(signatories)).To(Equal(0))
			})
		})
	})
})

func initDHT() (dht.DHT, id.Signatory) {
	privKey := id.NewPrivKey()
	identity := id.NewSignatory(&privKey.PublicKey)
	resolver := dht.NewDoubleCacheContentResolver(dht.DefaultDoubleCacheContentResolverOptions(), nil)
	return dht.New(identity, resolver), identity
}

func sortAddrs(identity id.Signatory, addrs []wire.Address) {
	sort.Slice(addrs, func(i, j int) bool {
		fstSignatory, err := addrs[i].Signatory()
		Expect(err).ToNot(HaveOccurred())

		sndSignatory, err := addrs[j].Signatory()
		Expect(err).ToNot(HaveOccurred())

		for b := 0; b < 32; b++ {
			d1 := identity[b] ^ fstSignatory[b]
			d2 := identity[b] ^ sndSignatory[b]
			if d1 < d2 {
				return true
			}
			if d2 < d1 {
				return false
			}
		}
		return false
	})
}

func sortSignatories(identity id.Signatory, signatories []id.Signatory) {
	sort.Slice(signatories, func(i, j int) bool {
		for b := 0; b < 32; b++ {
			d1 := identity[b] ^ signatories[i][b]
			d2 := identity[b] ^ signatories[j][b]
			if d1 < d2 {
				return true
			}
			if d2 < d1 {
				return false
			}
		}
		return false
	})
}
