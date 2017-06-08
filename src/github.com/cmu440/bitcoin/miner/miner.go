package main

import (
	"fmt"
	"os"
	"encoding/json"

	"github.com/cmu440/lsp"
	"github.com/cmu440/bitcoin"
)

func main() {
	const numArgs = 2
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./miner <hostport>")
		return
	}
	miner, err := lsp.NewClient(os.Args[1], lsp.NewParams())
	if err != nil {
		fmt.Println(err)
		printDisconnected()
		return
	}

	// Send Join request to server
	join := bitcoin.NewJoin()
	msg, err := json.Marshal(join)
	if err != nil {
		return
	}
	err = miner.Write(msg)
	if err != nil {
		printDisconnected()
		return
	}

	for {
		payload, err := miner.Read()
		if err != nil {
			printDisconnected()
			return
		}
		var req bitcoin.Message
		err = json.Unmarshal(payload, &req)
		if err != nil {
			continue
		}
		// fmt.Println(req)
	    var nonce uint64
		minHash := bitcoin.Hash(req.Data, req.Lower)
		for i := req.Lower; i <= req.Upper; i++ {
			hash := bitcoin.Hash(req.Data, i)
			if hash < minHash {
				minHash = hash
				nonce = i
			}
		}

		result := bitcoin.NewResult(minHash, nonce)
		msg, err := json.Marshal(result)
		if err != nil {
			continue
		}
		err = miner.Write(msg)
		if err != nil {
			printDisconnected()
			return
		}
	}
}

// printResult prints the final result to stdout.
func printResult(hash, nonce string) {
	fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	fmt.Println("Disconnected")
}
