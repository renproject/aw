package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"github.com/renproject/aw/conn"
)

func CreateSendReceiveServices(c net.Conn) (func([]byte) error, func([]byte) error) {
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

func echo(c net.Conn) {
	send, receive := CreateSendReceiveServices(c)
	for {
		var data [1024]byte
		err := receive(data[:])
		if err == nil {
			fmt.Print("Server received echo request: ", string(data[:]))
			send(data[:])
		}
	}
}

func RunEchoServer(address string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := conn.Listen(ctx, address, echo, errHandler, nil); err != nil {
		errHandler(err)
	}
}

func sendAndReceiveInput(c net.Conn) {
	send, receive := CreateSendReceiveServices(c)
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

func RunEchoClient(address string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := conn.Dial(ctx, address, sendAndReceiveInput, errHandler, nil); err != nil {
		errHandler(err)
	}
}

func main() {
	args := os.Args
	if len(args) < 3 {
		panic(fmt.Errorf("Insufficient arguments"))
	}

	switch args[1] {
	case "server":
		RunEchoServer(args[2])
	case "client":
		RunEchoClient(args[2])
	default:
		panic(fmt.Errorf("Invalid argument. Should specify server/client"))
	}
}
