package main

import (
	"fmt"
	"os"
	"strconv"
	"encoding/json"

	"github.com/cmu440/lsp"
	"github.com/cmu440/bitcoin"
)

func main() {
	const numArgs = 4
	if len(os.Args) != numArgs {
		fmt.Println("Usage: ./client <hostport> <message> <maxNonce>")
		return
	}

	message := os.Args[2]
	maxNonce, _ := strconv.ParseUint(os.Args[3], 10, 64)

	params := lsp.NewParams()
	client, err := lsp.NewClient(os.Args[1], params)
	if err != nil {
		printDisconnected()
		return
	}

	// Send request to server
	req := bitcoin.NewRequest(message, 0, maxNonce)
	msg, err := json.Marshal(req)
	if err != nil {
		return
	}

	err = client.Write(msg)
	if err != nil {
		printDisconnected()
		return
	}

	// Read result from server
	payload, err := client.Read()
	if err != nil {
		printDisconnected()
		return
	} else {
		var result bitcoin.Message
		err := json.Unmarshal(payload, &result)
		if err != nil {
			printDisconnected()
			return
		}

		hash := strconv.FormatUint(result.Hash, 10)
		nonce := strconv.FormatUint(result.Nonce, 10)
		printResult(hash, nonce)
	}

	// Client Exit
	client.Close()
}

// printResult prints the final result to stdout.
func printResult(hash, nonce string) {
	fmt.Println("Result", hash, nonce)
}

// printDisconnected prints a disconnected message to stdout.
func printDisconnected() {
	fmt.Println("Disconnected")
}
