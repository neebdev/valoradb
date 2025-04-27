package main

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/neebdev/valoradb/internal/config"
)

// main creates and starts the interactive client
func main() {
	// Setup signal handling for clean shutdown
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	
	// Try to determine the best location for the config file
	configPath := "config.json"
	
	// First check if a config exists in the current working directory
	if _, err := os.Stat(configPath); err == nil {
		fmt.Println("Using config.json from current working directory")
	} else {
		// If not found, try to get the executable directory
		execPath, err := os.Executable()
		if err == nil {
			execDir := filepath.Dir(execPath)
			
			// Don't use the temporary directory when running with 'go run'
			if !strings.Contains(execDir, "go-build") && !strings.Contains(execDir, "Temp") {
				configPath = filepath.Join(execDir, "config.json")
				fmt.Printf("Looking for config at executable path: %s\n", configPath)
			}
		}
	}
	
	// Load the configuration
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		fmt.Printf("Failed to load configuration: %v\n", err)
		return
	}
	
	fmt.Printf("Using WAL directory: %s\n", cfg.WAL.Directory)
	
	// Ensure WAL directory exists
	if err := cfg.EnsureWALDirectory(); err != nil {
		fmt.Printf("Failed to create WAL directory: %v\n", err)
		return
	}
	
	// Initialize the client
	client, err := NewClient(cfg)
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