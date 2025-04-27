package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

// main creates and starts the interactive client
func main() {
	// Setup signal handling for clean shutdown
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	
	walPath := "./wal.log"
	client, err := NewClient(walPath)
	if err != nil {
		fmt.Printf("Failed to initialize client: %v\n", err)
		return
	}
	defer client.store.Wal.Close()
	
	// Start a goroutine to handle graceful shutdown
	go func() {
		<-sigs
		fmt.Println("\nShutting down ValoraDĂ...")
		client.store.Wal.Close()
		os.Exit(0)
	}()

	// Start the interactive shell
	client.Start()
}