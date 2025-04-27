package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Config represents the database configuration
type Config struct {
	WAL WALConfig `json:"wal"`
}

// WALConfig contains WAL-specific configuration
type WALConfig struct {
	Directory   string `json:"directory"`   // Base directory for WAL files
	SegmentSize int64  `json:"segmentSize"` // Max size of each WAL segment in bytes (default: 16MB)
}

// DefaultConfig returns the default configuration
func DefaultConfig() *Config {
	return &Config{
		WAL: WALConfig{
			Directory:   "./data/wal",  // Store WAL files in a single directory
			SegmentSize: 16 * 1024 * 1024, // 16MB
		},
	}
}

// LoadConfig loads configuration from the specified file
// If the file doesn't exist, it creates a new one with default settings
func LoadConfig(configPath string) (*Config, error) {
	// First check if the file exists
	_, err := os.Stat(configPath)
	if os.IsNotExist(err) {
		// Create default config
		cfg := DefaultConfig()
		
		// Create directory if it doesn't exist
		dir := filepath.Dir(configPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create config directory: %w", err)
		}
		
		// Write default config to file
		if err := cfg.Save(configPath); err != nil {
			return nil, fmt.Errorf("failed to save default config: %w", err)
		}
		
		// Normalize paths
		normalizeConfigPaths(cfg, filepath.Dir(configPath))
		
		return cfg, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to check config file: %w", err)
	}
	
	// Read and parse the file
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}
	
	// Normalize paths
	normalizeConfigPaths(&cfg, filepath.Dir(configPath))
	
	return &cfg, nil
}

// normalizeConfigPaths converts relative paths in the config to absolute paths
// based on the directory where the config file is located
func normalizeConfigPaths(cfg *Config, configDir string) {
	// When running with 'go run', the configDir might be a temporary directory
	// Try to detect this case and use the working directory instead
	if strings.Contains(configDir, "go-build") || strings.Contains(configDir, "Temp") {
		// Use current working directory as a fallback
		if workingDir, err := os.Getwd(); err == nil {
			configDir = workingDir
			fmt.Printf("Using working directory for path resolution: %s\n", workingDir)
		}
	}

	// Normalize WAL directory path if it's relative
	if cfg.WAL.Directory != "" && !filepath.IsAbs(cfg.WAL.Directory) {
		// Remove ./ prefix if present for cleaner path resolution
		walDir := cfg.WAL.Directory
		if strings.HasPrefix(walDir, "./") {
			walDir = walDir[2:]
		}
		cfg.WAL.Directory = filepath.Join(configDir, walDir)
	}
}

// Save writes the configuration to the specified file
func (c *Config) Save(configPath string) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}
	
	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}
	
	return nil
}

// EnsureWALDirectory creates the WAL directory if it doesn't exist
func (c *Config) EnsureWALDirectory() error {
	if err := os.MkdirAll(c.WAL.Directory, 0755); err != nil {
		return fmt.Errorf("failed to create WAL directory: %w", err)
	}
	return nil
} 