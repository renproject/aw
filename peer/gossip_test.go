package peer_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/renproject/aw/peer"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Gossip", func() {
	Context("When a node is gossipping with peers", func() {
		It("should sync content correctly", func() {

			// Number of peers
			n := 4
			opts, peers, tables, contentResolvers, _, _ := setup(n)

			for i := range peers {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				go peers[i].Run(ctx)
				tables[i].AddPeer(opts[(i+1)%n].PrivKey.Signatory(),
					wire.NewUnsignedAddress(wire.TCP,
						fmt.Sprintf("%v:%v", "localhost", uint16(3333+i+1)), uint64(time.Now().UnixNano())))
				tables[(i+1)%n].AddPeer(opts[i].PrivKey.Signatory(),
					wire.NewUnsignedAddress(wire.TCP,
						fmt.Sprintf("%v:%v", "localhost", uint16(3333+i)), uint64(time.Now().UnixNano())))
			}
			for i := range peers {
				msgHello := fmt.Sprintf("Hi from %v", peers[i].ID().String())
				contentID := id.NewHash([]byte(msgHello))
				contentResolvers[i].InsertContent(contentID[:], []byte(msgHello))
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				peers[i].Gossip(ctx, contentID[:], &peer.DefaultSubnet)
			}
			<-time.After(5 * time.Second)
			for i := range peers {
				for j := range peers {
					msgHello := fmt.Sprintf("Hi from %v", peers[j].ID().String())
					contentID := id.NewHash([]byte(msgHello))
					_, ok := contentResolvers[i].QueryContent(contentID[:])
					Expect(ok).To(BeTrue())
				}
			}
		})

		It("should not send to itself", func() {
			// Custom logger that writes the error logs to a file
			highPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
				return lvl >= zapcore.ErrorLevel
			})
			lowPriority := zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
				return lvl < zapcore.ErrorLevel
			})

			consoleDebugging := zapcore.Lock(os.Stdout)
			consoleErrors, err := os.Create("err")
			if err != nil {
				panic(err)
			}
			defer os.Remove("err")
			consoleEncoder := zapcore.NewConsoleEncoder(zap.NewDevelopmentEncoderConfig())

			core := zapcore.NewTee(
				zapcore.NewCore(consoleEncoder, consoleErrors, highPriority),
				zapcore.NewCore(consoleEncoder, consoleDebugging, lowPriority),
			)
			logger := zap.New(core)
			opts, peers, tables, contentResolvers, _, _ := setupWithLogger(1, logger)

			// Add the peer's own address to the peer table. If this regression
			// has been fixed, then the call to AddPeer should actually return
			// early and not add an entry to the table.
			tables[0].AddPeer(opts[0].PrivKey.Signatory(),
				wire.NewUnsignedAddress(wire.TCP,
					fmt.Sprintf("%v:%v", "localhost", uint16(3333)), uint64(time.Now().UnixNano())))

			for i := range peers {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				go peers[i].Run(ctx)
			}

			for i := range peers {
				msgHello := fmt.Sprintf("Hi from %v", peers[i].ID().String())
				contentID := id.NewHash([]byte(msgHello))
				contentResolvers[i].InsertContent(contentID[:], []byte(msgHello))
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				peers[i].Gossip(ctx, contentID[:], &peer.DefaultSubnet)
			}

			// FIXME(ross): If we don't wait long enough, the peer will not
			// have sent a message to itself yet. The time this takes should be
			// very small; the fact that this can still have not occurred after
			// a whole second indicates that something is wrong.
			<-time.After(5 * time.Second)

			buf := make([]byte, 1024)
			n, err := consoleErrors.ReadAt(buf, 0)
			if err != nil && err != io.EOF {
				panic(err)
			}
			fmt.Printf("%s\n", string(buf[:n]))

			// If a peer sends a message to themself, there will be a decoding
			// error.
			Expect(strings.Contains(string(buf[:n]), "message authentication failed")).To(BeFalse())
		})
	})
})
