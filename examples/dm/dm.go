package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"

	"github.com/renproject/aw/conn"
)

func CreateSendReceiveServicesServer() (func(net.Conn, []byte) error, func(net.Conn, []byte) error) {
	return func(c net.Conn, data []byte) error {
			if _, err := c.Write(data); err != nil {
				return err
			}
			return nil
		},
		func(c net.Conn, data []byte) error {
			if _, err := c.Read(data); err != nil {
				return err
			}
			return nil
		}
}

func CreateSendReceiveServicesClient(c net.Conn) (func([]byte) error, func([]byte) error) {
	return func(data []byte) error {
			if _, err := c.Write(data); err != nil {
				return err
			}
			return nil
		},
		func(data []byte) error {
			if _, err := c.Read(data); err != nil {
				return err
			}
			return nil
		}
}

func errHandler(err error) {
	log.Println(err)
}

func createServerConnHandler() func(c net.Conn) {
	addressMap := make(map[string]net.Conn)
	send, receive := CreateSendReceiveServicesServer()
	return func(c net.Conn) {
		senderAddressAsString := c.RemoteAddr().String()
		addressMap[senderAddressAsString] = c

	L1:
		for {
			var data [1024]byte
			err := receive(c, data[:])
			if err != nil {
				continue
			}
			dataAsStrings := strings.Split(string(data[:]), "->")
			if len(dataAsStrings) < 2 {
				send(c, []byte("Server: Invalid Message\n"))
				continue L1
			}
			receiverConn, ok := addressMap[dataAsStrings[0]]
			if !ok {
				switch dataAsStrings[0] {
				case "broadcast":
					for _, receiverConn := range addressMap {
						send(receiverConn, append([]byte(senderAddressAsString), []byte(dataAsStrings[1])...))
					}
				case "?peers":
					addressesAsStrings := []string{"server: Addresses Found"}
					for str := range addressMap {
						addressesAsStrings = append(addressesAsStrings, str)
					}
					send(c, []byte(strings.Join(addressesAsStrings, "\n\t")+"\n"))
				default:
					send(c, []byte("Server: Invalid Receiver\n"))
				}
				continue L1
			}

			send(receiverConn, append([]byte(senderAddressAsString), []byte(dataAsStrings[1])...))
		}
	}
}

func RunServer(address string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := conn.Listen(ctx, address, createServerConnHandler(), errHandler, nil); err != nil {
		errHandler(err)
	}
}

func clientConnHandler(c net.Conn) {
	send, receive := CreateSendReceiveServicesClient(c)
	go func() {
		reader := bufio.NewReader(os.Stdin)
		for {
			input, err := reader.ReadString('\n')
			if err != nil {
				errHandler(err)
				continue
			}
			send([]byte(input))
		}
	}()

	for {
		output := make([]byte, 1024)
		if err := receive(output[:]); err != nil {
			if err == io.EOF {
				return
			}
			errHandler(err)
			continue
		}
		fmt.Print(string(output[:]))
	}
}

func RunClient(address string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := conn.Dial(ctx, address, clientConnHandler, errHandler, nil); err != nil {
		errHandler(err)
	}
}

func main() {
	args := os.Args[1:]

	switch args[0] {
	case "server":
		RunServer(args[1])
	case "client":
		RunClient(args[1])
	}
}
