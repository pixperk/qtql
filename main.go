package main

import (
	"bufio"
	"log"
	"os"
	"strings"
)

func main() {
	reader := bufio.NewReader(os.Stdin)

	store := New_Store("kvs_wal.log")

	if err := store.Replay_wal(); err != nil {
		log.Fatalf("Failed to replay WAL: %v", err)
	}

	for {
		println("kvs > [q to quit]: ")

		line, err := reader.ReadString('\n')
		if err != nil {
			println("error reading input:", err.Error())
			continue
		}

		line = strings.TrimSpace(line)

		if line == "" {
			continue
		}

		if line == "q" {
			println("exiting...")
			break
		}

		tokens := strings.Split(line, " ")

		if err := store.Process(tokens); err != nil {
			log.Printf("Error: %v\n", err)
		}

	}
}
