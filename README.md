# `🌪 airwave`

[![GoDoc](https://godoc.org/github.com/renproject/aw?status.svg)](https://godoc.org/github.com/renproject/aw)
[![CircleCI](https://circleci.com/gh/renproject/aw/tree/master.svg?style=shield)](https://circleci.com/gh/renproject/aw/tree/master)
![Go Report](https://goreportcard.com/badge/github.com/renproject/aw)
[![Coverage Status](https://coveralls.io/repos/github/renproject/aw/badge.svg?branch=master)](https://coveralls.io/github/renproject/aw?branch=master)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](https://opensource.org/licenses/MIT)


A flexible P2P networking library for upgradable distributed systems. The core mission of `airwave` is to provide a simple P2P interface that can support a wide variety of different algorithms, with a focus on backwards compatible. The P2P interface supports:

- Peer discovery
- Handshake 
- Casting (send to one)
- Multicasting (send to many)
- Broadcasting (send to everyone)

### Handshake

Airwave uses a 3 way sync handshake method to authorize peers in the network. The process is as follows:

![](docs/handshake.svg)

The client sends a signed rsa public key on connect. The server validates the signature, generates a random challenge, and sends the signed random challenge encrypted with the client's public key; and the server's public key. The client validates the server's signature decrypts the challenge encrypts it with the server's publickey, signs it and sends it back.

Built with ❤ by Ren. 
