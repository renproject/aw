package dht_test

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"testing/quick"
	"time"

	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/dht/dhtutil"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"go.uber.org/zap"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gmeasure"
)

var _ = Describe("DHT", func() {
	Describe("Addresses", func() {
		Context("when inserting an address", func() {
			It("should be able to query it", func() {
				table, _ := initDHT()

				f := func(seed int64) bool {
					privKey := id.NewPrivKey()
					sig := privKey.Signatory()
					addr := wire.NewUnsignedAddress(wire.TCP, "172.16.254.1:3000", uint64(time.Now().UnixNano()))
					err := addr.Sign(privKey)
					if err != nil {
						panic(err)
					}

					table.AddPeer(sig, addr)

					signatory := id.NewSignatory((*id.PubKey)(&privKey.PublicKey))
					newAddr, ok := table.PeerAddress(signatory)
					Expect(ok).To(BeTrue())
					Expect(newAddr).To(Equal(addr))
					return true
				}
				Expect(quick.Check(f, nil)).To(Succeed())
			})
		})

		Context("when deleting an address", func() {
			It("should not be able to query it", func() {
				table, _ := initDHT()

				f := func(seed int64) bool {
					privKey := id.NewPrivKey()
					addr := wire.NewUnsignedAddress(wire.TCP, "172.16.254.1:3000", uint64(time.Now().UnixNano()))
					err := addr.Sign(privKey)
					if err != nil {
						panic(err)
					}

					// Try to delete the address prior to inserting to make sure
					// it does not panic.
					signatory := id.NewSignatory((*id.PubKey)(&privKey.PublicKey))
					table.DeletePeer(signatory)

					// Insert the address.
					table.AddPeer(signatory, addr)

					// Delete the address and make sure it no longer exists when
					// querying the DHT.
					table.DeletePeer(signatory)

					_, ok := table.PeerAddress(signatory)
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
				signatories := make([]id.Signatory, numAddrs)
				for i := 0; i < numAddrs; i++ {
					privKey := id.NewPrivKey()
					sig := privKey.Signatory()
					addr := wire.NewUnsignedAddress(wire.TCP, "172.16.254.1:3000", uint64(time.Now().UnixNano()))
					err := addr.Sign(privKey)
					if err != nil {
						panic(err)
					}

					table.AddPeer(sig, addr)

					signatories = append(signatories, sig)
				}

				// Check addresses are returned in order of their XOR distance
				// from our own address.
				numQueriedAddrs := rand.Intn(numAddrs)
				queriedAddrs := table.Peers(numQueriedAddrs)
				Expect(len(queriedAddrs)).To(Equal(numQueriedAddrs))
				Expect(dhtutil.IsSorted(identity, queriedAddrs)).To(BeTrue())

				// Delete some addresses and make sure the list is still sorted.
				numDeletedAddrs := rand.Intn(numAddrs)
				for i := 0; i < numDeletedAddrs; i++ {
					signatory := signatories[i]
					table.DeletePeer(signatory)
				}

				queriedAddrs = table.Peers(numAddrs - numDeletedAddrs)
				Expect(len(queriedAddrs)).To(Equal(numAddrs - numDeletedAddrs))
				Expect(dhtutil.IsSorted(identity, queriedAddrs)).To(BeTrue())
			})

			Context("if there are less than n addresses in the store", func() {
				It("should return all the addresses", func() {
					table, _ := initDHT()
					numAddrs := rand.Intn(100)

					// Insert `numAddrs` random addresses into the store.
					for i := 0; i < numAddrs; i++ {
						privKey := id.NewPrivKey()
						sig := privKey.Signatory()
						addr := wire.NewUnsignedAddress(wire.TCP, "172.16.254.1:3000", uint64(time.Now().UnixNano()))
						err := addr.Sign(privKey)
						if err != nil {
							panic(err)
						}

						table.AddPeer(sig, addr)
					}

					addrs := table.Peers(100)
					Expect(len(addrs)).To(Equal(numAddrs))
				})
			})

			Context("if there are no addresses in the store", func() {
				It("should return no addresses", func() {
					table, _ := initDHT()

					addrs := table.Peers(100)
					Expect(len(addrs)).To(Equal(0))

					addrs = table.Peers(0)
					Expect(len(addrs)).To(Equal(0))
				})
			})
		})

		Context("when querying random peers", func() {
			It("should return the correct amount", func() {
				table, _ := initDHT()

				f := func(seed int64) bool {
					numAddrs := rand.Intn(11000)

					// Insert `numAddrs` random addresses into the store.
					for i := 0; i < numAddrs; i++ {
						privKey := id.NewPrivKey()
						sig := privKey.Signatory()
						addr := wire.NewUnsignedAddress(wire.TCP, "172.16.254.1:3000", uint64(time.Now().UnixNano()))
						err := addr.Sign(privKey)
						if err != nil {
							panic(err)
						}

						table.AddPeer(sig, addr)
					}

					numRandomAddrs := rand.Intn(numAddrs)
					randomAddr := table.RandomPeers(numRandomAddrs)
					Expect(len(randomAddr)).To(Equal(numRandomAddrs))
					return true
				}

				Expect(quick.Check(f, &quick.Config{MaxCount: 10})).To(Succeed())
			})

			Context("where the requested number is larger than the number of peers present in table", func() {
				It("should return the number of peers in table", func() {
					table, _ := initDHT()
					numAddrs := rand.Intn(100)

					// Insert `numAddrs` random addresses into the store.
					for i := 0; i < numAddrs; i++ {
						privKey := id.NewPrivKey()
						sig := privKey.Signatory()
						addr := wire.NewUnsignedAddress(wire.TCP, "172.16.254.1:3000", uint64(time.Now().UnixNano()))
						err := addr.Sign(privKey)
						if err != nil {
							panic(err)
						}

						table.AddPeer(sig, addr)
					}

					randomAddr := table.RandomPeers(numAddrs + rand.Intn(100))
					Expect(len(randomAddr)).To(Equal(numAddrs))

				})
			})

			It("should return a unique subset each time", func() {
				table, _ := initDHT()
				numAddrs := rand.Intn(100)
				numRandAddrs := rand.Intn(numAddrs)

				// Insert `numAddrs` random addresses into the store.
				for i := 0; i < numAddrs; i++ {
					privKey := id.NewPrivKey()
					sig := privKey.Signatory()
					addr := wire.NewUnsignedAddress(wire.TCP, "172.16.254.1:3000", uint64(time.Now().UnixNano()))
					err := addr.Sign(privKey)
					if err != nil {
						panic(err)
					}

					table.AddPeer(sig, addr)
				}

				lists := make([][]id.Signatory, 10)
				for i := range lists {
					lists[i] = table.RandomPeers(numRandAddrs)
				}

				for i := 0; i < 10; i++ {
					for j := i + 1; j < 10; j++ {
						Expect(lists[i]).To(Not(Equal(lists[j])))
					}
				}
			})

			It("should work while deleting peers from the table", func() {
				table, _ := initDHT()
				numAddrs := rand.Intn(99) + 1
				numRandAddrs := rand.Intn(numAddrs)

				// Insert `numAddrs` random addresses into the store.
				deletedPeers := make([]id.Signatory, 0, 50)
				for i := 0; i < numAddrs; i++ {
					privKey := id.NewPrivKey()
					sig := privKey.Signatory()
					addr := wire.NewUnsignedAddress(wire.TCP, "172.16.254.1:3000", uint64(time.Now().UnixNano()))
					err := addr.Sign(privKey)
					if err != nil {
						panic(err)
					}
					table.AddPeer(sig, addr)
					if i < numAddrs/2 {
						deletedPeers = append(deletedPeers, sig)
					}
				}

				done := make(chan struct{}, 1)
				go func() {
					defer close(done)

					for i := range deletedPeers {
						table.DeletePeer(deletedPeers[i])
					}
				}()

				total := time.Duration(0)
				for i := 0; i < 50; i++ {
					start := time.Now()
					table.RandomPeers(numRandAddrs)
					duration := time.Now().Sub(start)
					total += duration
				}
				log.Printf("RandomPeers takes %v on average", total/50)
				<-done
			})
		})

		Context("when querying the number of addresses", func() {
			It("should return the correct amount", func() {
				table, _ := initDHT()
				numAddrs := rand.Intn(100)

				// Insert `numAddrs` random addresses into the store.
				for i := 0; i < numAddrs; i++ {
					privKey := id.NewPrivKey()
					sig := privKey.Signatory()
					addr := wire.NewUnsignedAddress(wire.TCP, "172.16.254.1:3000", uint64(time.Now().UnixNano()))
					err := addr.Sign(privKey)
					if err != nil {
						panic(err)
					}

					table.AddPeer(sig, addr)
				}

				n := table.NumPeers()
				Expect(n).To(Equal(numAddrs))
			})
		})

		Context("when re-inserting an address", func() {
			It("the sorted list of signatories should remain unchanged", func() {

				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				f := func(seed int64) bool {
					table, _ := initDHT()
					numPeers := r.Intn(100) + 1
					randomPeerIndex := r.Intn(numPeers)
					var randomPeerPrivKey *id.PrivKey
					for i := 0; i < numPeers; i++ {
						privKey := id.NewPrivKey()
						if i == randomPeerIndex {
							randomPeerPrivKey = privKey
						}
						sig := privKey.Signatory()
						ipAddr := fmt.Sprintf("%d.%d.%d.%d:%d",
							r.Intn(256), r.Intn(256), r.Intn(256), r.Intn(256), r.Intn(65536))
						addr := wire.NewUnsignedAddress(wire.TCP, ipAddr, uint64(time.Now().UnixNano()))
						err := addr.Sign(privKey)
						if err != nil {
							panic(err)
						}
						table.AddPeer(sig, addr)
					}

					peers := table.Peers(numPeers + 10)
					randomSig := randomPeerPrivKey.Signatory()
					newIPAddr := fmt.Sprintf("%d.%d.%d.%d:%d",
						r.Intn(256), r.Intn(256), r.Intn(256), r.Intn(256), r.Intn(65536))
					newAddr := wire.NewUnsignedAddress(wire.TCP, newIPAddr, uint64(time.Now().UnixNano()))
					err := newAddr.Sign(randomPeerPrivKey)
					if err != nil {
						panic(err)
					}
					ok := table.AddPeer(randomSig, newAddr)
					if !ok {
						panic("peer not added to table")
					}

					newPeers := table.Peers(numPeers + 1)
					Expect(len(newPeers)).To(Equal(numPeers))
					Expect(peers).To(Equal(newPeers))
					return true
				}
				Expect(quick.Check(f, nil)).To(Succeed())
			})
		})

		It("measures adding 10000 addresses to distributed hash table", func() {
			experiment := gmeasure.NewExperiment("adding 10000 addresses to distributed hash table")
			AddReportEntry(experiment.Name, experiment)

			table, _ := initDHT()
			signatories := make([]id.Signatory, 0)
			for i := 0; i < 10000; i++ {
				privKey := id.NewPrivKey()
				sig := privKey.Signatory()
				signatories = append(signatories, sig)
			}
			experiment.MeasureDuration("runtime", func() {
				for i := 0; i < len(signatories); i++ {
					addr := wire.NewUnsignedAddress(wire.TCP, "172.16.254.1:"+strconv.Itoa(i), uint64(time.Now().UnixNano()))
					table.AddPeer(signatories[i], addr)
				}
			})
		})

		It("mesasures removing 10000 addresses from distributed hash table", func() {
			experiment := gmeasure.NewExperiment("removing 10000 addresses from distributed hash table")
			AddReportEntry(experiment.Name, experiment)

			table, _ := initDHT()
			signatories := make([]id.Signatory, 0)
			for i := 0; i < 10000; i++ {
				privKey := id.NewPrivKey()
				sig := privKey.Signatory()
				addr := wire.NewUnsignedAddress(wire.TCP, "172.16.254.1:"+strconv.Itoa(i), uint64(time.Now().UnixNano()))
				table.AddPeer(sig, addr)
				signatories = append(signatories, sig)
			}
			experiment.MeasureDuration("runtime", func() {
				for i := 0; i < len(signatories); i++ {
					table.DeletePeer(signatories[i])
				}
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
					signatories[i] = id.NewSignatory((*id.PubKey)(&privKey.PublicKey))
				}

				hash := table.AddSubnet(signatories)
				newSignatories := table.Subnet(hash)

				// Sort the original slice by XOR distance from our address and
				// verify it is equal to the result.
				dhtutil.SortSignatories(identity, signatories)
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
					signatories[i] = id.NewSignatory((*id.PubKey)(&privKey.PublicKey))
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

func initDHT() (dht.Table, id.Signatory) {
	loggerConfig := zap.NewDevelopmentConfig()
	loggerConfig.Level.SetLevel(zap.WarnLevel)
	logger, err := loggerConfig.Build()
	if err != nil {
		panic(err)
	}

	privKey := id.NewPrivKey()
	identity := id.NewSignatory((*id.PubKey)(&privKey.PublicKey))
	return dht.NewInMemTable(identity, logger), identity
}
