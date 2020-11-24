package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"github.com/renproject/aw/channel"
	"github.com/renproject/aw/dht"
	"github.com/renproject/aw/handshake"
	"github.com/renproject/aw/peer"
	"github.com/renproject/aw/transport"
	"github.com/renproject/aw/wire"
	"github.com/renproject/id"
	"go.uber.org/zap"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

func CLI(p *peer.Peer, gossip peer.GossipFunc) {
	reader := bufio.NewReader(os.Stdin)
	for {
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("error reading user input: %v\n", err)
			continue
		}

		trimmedInput := strings.TrimSpace(input)
		switch string(trimmedInput[0]) {
		case ">":
			if err := handleInstruction(p, trimmedInput[1:]); err != nil {
				fmt.Printf("%v\n", err)
			}
		case "@":
			if err := handleDirectMessage(p, trimmedInput[1:]); err != nil {
				fmt.Printf("%v\n", err)
			}
		case "#":
			handleBroadcastToRoom(p, trimmedInput[1:], gossip)
		// case "*":
		// 	if err := handleGlobalBroadcast(p, trimmedInput[1:]); err != nil {
		// 		fmt.Printf("%v\n", err)
		// 	}
		default:
			fmt.Println("err - Invalid msg Prefix")
		}
	}
}

func handleInstruction(p *peer.Peer, data string) error {
	trimmedData := strings.TrimLeft(data, " ")
	splitIndex := strings.Index(trimmedData, " ")

	if splitIndex == -1 {
		switch trimmedData {
		case "ping":
			return fmt.Errorf("err - ping is unimplemented")
		case "info":
			fmt.Printf("My ID: %v\n", p.ID().String())
			fmt.Printf("Currently added peers: \n")
			for _, x := range p.Table().Addresses(p.Table().NumPeers()) {
				fmt.Printf("\t%v\n", x.String())
			}
			fmt.Println()
			return nil
		default:
			return fmt.Errorf("err - invalid instruction")
		}
	}

	switch trimmedData[:splitIndex] {
	case "leave":
		return fmt.Errorf("err - leave is unimplemented")
	case "add":
		args := strings.Fields(data[splitIndex:])
		if len(args) != 2 {
			return fmt.Errorf("err - invalid arguments to addition instruction")
		}

		sig := [32]byte{}
		decodedBytes, err := base64.RawURLEncoding.DecodeString(strings.TrimLeft(args[0], " "))
		if err != nil {
			return fmt.Errorf("err - string id could not be decoded")
		}
		copy(sig[:], decodedBytes)
		p.Table().AddPeer(sig, args[1])
	case "del":
		sig := [32]byte{}
		decodedBytes, err := base64.RawURLEncoding.DecodeString(strings.TrimLeft(data[splitIndex:], " "))
		if err != nil {
			return fmt.Errorf("err - string id could not be decoded")
		}
		copy(sig[:], decodedBytes)
		p.Table().DeletePeer(sig)
	case "get":
		sig := [32]byte{}
		decodedBytes, err := base64.RawURLEncoding.DecodeString(strings.TrimLeft(data[splitIndex:], " "))
		if err != nil {
			return fmt.Errorf("err - string id could not be decoded")
		}
		copy(sig[:], decodedBytes)
		address, ok := p.Table().PeerAddress(sig)
		if !ok {
			return fmt.Errorf("err - unknown peer id: %v", id.Signatory(sig).String())
		}
		fmt.Printf("Address associated with peer is: %v\n", address)
	default:
		return fmt.Errorf("err - invalid instruction")
	}
	return nil
}

func handleDirectMessage(p *peer.Peer, data string) error {
	trimmedData := strings.TrimLeft(data, " ")
	splitIndex := strings.Index(trimmedData, " ")

	if splitIndex == -1 {
		return fmt.Errorf("err - empty message")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sig := [32]byte{}
	decodedBytes, err := base64.RawURLEncoding.DecodeString(data[:splitIndex])
	if err != nil {
		return fmt.Errorf("err - string id could not be decoded")
	}

	copy(sig[:], decodedBytes)
	msg := wire.Msg{Version: wire.MsgVersion1, Type: wire.MsgTypeSend, Data: []byte(strings.TrimLeft(data[splitIndex:], " "))}
	if err := p.Send(ctx, sig, msg); err != nil {
		fmt.Printf("%v", err)
		return fmt.Errorf("err - message could not be sent")
	}

	return nil
}

func handleBroadcastToRoom(p *peer.Peer, data string, gossip peer.GossipFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("Sending data : ", string([]byte(data)))
	hash := id.NewHash([]byte(data))
	p.ContentResolver().Insert(dht.ContentID(hash), []byte(data))
	if err := gossip(ctx, peer.GlobalSubnet, hash[:]); err != nil {
		p.Logger().Error("gossip broadcast DAT", zap.Error(err))
	}
}

// func handleGlobalBroadcast(p *peer.Peer, data string) error {
// 	ctx, cancel := context.WithCancel(context.Background())
// 	for sig, _ := range p.Options() {
// 		p.Send(ctx, sig, data)
// 	}
// }

func main() {

	args := os.Args
	num, _ := strconv.Atoi(args[1])
	port := uint16(num)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	loggerConfig := zap.NewProductionConfig()
	loggerConfig.Level.SetLevel(zap.DebugLevel)
	logger, err := loggerConfig.Build()
	if err != nil {
		panic(err)
	}

	key := id.NewPrivKey()
	self := key.Signatory()
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	h := handshake.ECIES(key, r)
	//h := handshake.Insecure(self)
	contentResolver := dht.NewDoubleCacheContentResolver(dht.DefaultDoubleCacheContentResolverOptions(), nil)
	client := channel.NewClient(
		channel.DefaultClientOptions().
			WithLogger(logger),
		self,
		func(msg wire.Msg) bool {
			if msg.Type == wire.MsgTypeSync {
				var contentID dht.ContentID
				copy(contentID[:], msg.Data)
				_, ok := contentResolver.Content(contentID)
				return ok
			}
			return true
		})

	table := dht.NewInMemTable(self)

	t := transport.New(
		transport.DefaultOptions().
			WithLogger(logger).
			WithPort(port).
			WithOncePoolOptions(handshake.OncePoolOptions{MinimumExpiryAge: 5 * time.Second}).
			WithClientTimeout(10 * time.Minute).
			WithServerTimeout(10 * time.Minute),
		self,
		client,
		h,
		table)

	callbackFunc, gossip := peer.Gossiper(logger, t, contentResolver, table, 10,
		peer.Callbacks{
			DidReceiveMessage: func(from id.Signatory, msg wire.Msg) {
				switch msg.Type {
				case wire.MsgTypeSend:
					println(string(msg.Data))
				case wire.MsgTypeSync:
					println(string(msg.SyncData))
				}
			},
		})
	opts := peer.DefaultOptions().WithLogger(logger).WithCallbacks(callbackFunc)
	p := peer.New(opts, t, contentResolver)
	go p.Run(ctx)
	CLI(p, gossip)
}
