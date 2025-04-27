package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	// DefaultMaxWalSize is the maximum size of a WAL file before rotation (64MB)
	DefaultMaxWalSize = 64 * 1024 * 1024
	
	// DefaultCheckpointInterval is the default interval between checkpoints (5 minutes)
	DefaultCheckpointInterval = 5 * time.Minute
	
	// DefaultBatchSize is the default number of records to batch before flushing
	DefaultBatchSize = 100
	
	// DefaultBatchWaitTime is the maximum time to wait before flushing a batch
	DefaultBatchWaitTime = 50 * time.Millisecond
	
	// DefaultWalFilePrefix is the prefix for WAL files
	DefaultWalFilePrefix = "wal"
	
	// DefaultWalFileExt is the extension for WAL files
	DefaultWalFileExt = ".log"
)

// WALConfig contains configuration options for the WAL
type WALConfig struct {
	// Directory where WAL files are stored
	Dir string
	
	// Maximum size of a WAL file before rotation
	MaxWalSize int64
	
	// Interval between checkpoints
	CheckpointInterval time.Duration
	
	// Number of records to batch before flushing
	BatchSize int
	
	// Maximum time to wait before flushing a batch
	BatchWaitTime time.Duration
	
	// WAL file prefix
	WalFilePrefix string
	
	// WAL file extension
	WalFileExt string
}

// Manager manages the write-ahead log
type Manager struct {
	config WALConfig
	
	// Current WAL file
	currentFile *os.File
	
	// Current WAL file size
	currentSize int64
	
	// Current WAL file number
	currentFileNum int
	
	// Current LSN
	currentLSN LogSequenceNumber
	
	// Last checkpoint LSN
	lastCheckpointLSN LogSequenceNumber
	
	// Buffered writer for the current WAL file
	writer *bufio.Writer
	
	// Mutex for WAL operations
	mu sync.Mutex
	
	// Record batch for group commit
	batch []Record
	batchMu sync.Mutex
	
	// Channel for batch flushing
	flushCh chan struct{}
	
	// Channel for checkpointing
	checkpointCh chan struct{}
	
	// Wait group for background goroutines
	wg sync.WaitGroup
	
	// Flag to indicate shutdown
	shutdown bool
	
	// Callback for checkpoints
	checkpointCallback func(LogSequenceNumber) error
}

// NewManager creates a new WAL manager
func NewManager(config WALConfig, checkpointCallback func(LogSequenceNumber) error) (*Manager, error) {
	// Use default config values if not specified
	if config.MaxWalSize <= 0 {
		config.MaxWalSize = DefaultMaxWalSize
	}
	if config.CheckpointInterval <= 0 {
		config.CheckpointInterval = DefaultCheckpointInterval
	}
	if config.BatchSize <= 0 {
		config.BatchSize = DefaultBatchSize
	}
	if config.BatchWaitTime <= 0 {
		config.BatchWaitTime = DefaultBatchWaitTime
	}
	if config.WalFilePrefix == "" {
		config.WalFilePrefix = DefaultWalFilePrefix
	}
	if config.WalFileExt == "" {
		config.WalFileExt = DefaultWalFileExt
	}
	
	// Create WAL directory if it doesn't exist
	if err := os.MkdirAll(config.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %w", err)
	}
	
	manager := &Manager{
		config:            config,
		currentLSN:        1, // Start LSN from 1
		batch:             make([]Record, 0, config.BatchSize),
		flushCh:           make(chan struct{}, 1),
		checkpointCh:      make(chan struct{}, 1),
		checkpointCallback: checkpointCallback,
	}
	
	// Find the highest existing WAL file number
	pattern := filepath.Join(config.Dir, fmt.Sprintf("%s_*.%s", config.WalFilePrefix, config.WalFileExt))
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to list WAL files: %w", err)
	}
	
	highestNum := 0
	for _, file := range files {
		var num int
		_, err := fmt.Sscanf(filepath.Base(file), fmt.Sprintf("%s_%%d.%s", config.WalFilePrefix, config.WalFileExt), &num)
		if err == nil && num > highestNum {
			highestNum = num
		}
	}
	
	manager.currentFileNum = highestNum
	
	// Create or open the current WAL file
	if err := manager.rotateLogIfNeeded(); err != nil {
		return nil, fmt.Errorf("failed to initialize WAL file: %w", err)
	}
	
	// Start background goroutines
	manager.wg.Add(2)
	go manager.batchFlusher()
	go manager.checkpointer()
	
	return manager, nil
}

// Shutdown gracefully shuts down the WAL manager
func (m *Manager) Shutdown() error {
	m.mu.Lock()
	if m.shutdown {
		m.mu.Unlock()
		return nil
	}
	m.shutdown = true
	m.mu.Unlock()
	
	// Trigger final flush
	m.Flush()
	
	// Signal background goroutines to stop
	close(m.flushCh)
	close(m.checkpointCh)
	
	// Wait for background goroutines to stop
	m.wg.Wait()
	
	// Close current WAL file
	if m.currentFile != nil {
		return m.currentFile.Close()
	}
	
	return nil
}

// AppendRecord appends a record to the WAL
func (m *Manager) AppendRecord(record Record) (LogSequenceNumber, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.shutdown {
		return 0, fmt.Errorf("WAL manager is shut down")
	}
	
	// Assign LSN to the record
	record.Header.LSN = m.currentLSN
	m.currentLSN++
	
	// Calculate checksum
	checksum := crc32.ChecksumIEEE(record.Payload)
	record.Header.Checksum = checksum
	
	// Add record to batch
	m.batchMu.Lock()
	m.batch = append(m.batch, record)
	batchSize := len(m.batch)
	m.batchMu.Unlock()
	
	// Trigger flush if batch is full
	if batchSize >= m.config.BatchSize {
		select {
		case m.flushCh <- struct{}{}:
			// Flush signal sent
		default:
			// Channel is full, flush is already scheduled
		}
	}
	
	return record.Header.LSN, nil
}

// Flush flushes any buffered records to disk
func (m *Manager) Flush() error {
	select {
	case m.flushCh <- struct{}{}:
		// Flush signal sent
	default:
		// Channel is full, flush is already scheduled
	}
	return nil
}

// Checkpoint triggers a checkpoint
func (m *Manager) Checkpoint() error {
	select {
	case m.checkpointCh <- struct{}{}:
		// Checkpoint signal sent
	default:
		// Channel is full, checkpoint is already scheduled
	}
	return nil
}

// GetCurrentLSN returns the current LSN
func (m *Manager) GetCurrentLSN() LogSequenceNumber {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.currentLSN
}

// GetLastCheckpointLSN returns the LSN of the last checkpoint
func (m *Manager) GetLastCheckpointLSN() LogSequenceNumber {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastCheckpointLSN
}

// batchFlusher is a background goroutine that flushes batched records
func (m *Manager) batchFlusher() {
	defer m.wg.Done()
	
	ticker := time.NewTicker(m.config.BatchWaitTime)
	defer ticker.Stop()
	
	for {
		select {
		case <-m.flushCh:
			if err := m.flushBatch(); err != nil {
				// Log error but continue
				fmt.Printf("Error flushing WAL batch: %v\n", err)
			}
		case <-ticker.C:
			// Time-based flush
			if err := m.flushBatch(); err != nil {
				// Log error but continue
				fmt.Printf("Error flushing WAL batch: %v\n", err)
			}
		}
		
		if m.shutdown {
			return
		}
	}
}

// checkpointer is a background goroutine that creates checkpoints
func (m *Manager) checkpointer() {
	defer m.wg.Done()
	
	ticker := time.NewTicker(m.config.CheckpointInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-m.checkpointCh:
			if err := m.createCheckpoint(); err != nil {
				// Log error but continue
				fmt.Printf("Error creating checkpoint: %v\n", err)
			}
		case <-ticker.C:
			// Time-based checkpoint
			if err := m.createCheckpoint(); err != nil {
				// Log error but continue
				fmt.Printf("Error creating checkpoint: %v\n", err)
			}
		}
		
		if m.shutdown {
			return
		}
	}
}

// flushBatch flushes the current batch of records to disk
func (m *Manager) flushBatch() error {
	// Get the current batch
	m.batchMu.Lock()
	if len(m.batch) == 0 {
		m.batchMu.Unlock()
		return nil
	}
	records := m.batch
	m.batch = make([]Record, 0, m.config.BatchSize)
	m.batchMu.Unlock()
	
	// Acquire mutex for file operations
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Check if we need to rotate the WAL file
	if err := m.rotateLogIfNeeded(); err != nil {
		return err
	}
	
	// Write all records
	for _, record := range records {
		// Write header
		if err := WriteHeader(m.writer, record.Header); err != nil {
			return err
		}
		
		// Write payload
		if _, err := m.writer.Write(record.Payload); err != nil {
			return err
		}
		
		// Update current size
		m.currentSize += int64(HeaderSize + len(record.Payload))
	}
	
	// Flush to disk
	if err := m.writer.Flush(); err != nil {
		return err
	}
	if err := m.currentFile.Sync(); err != nil {
		return err
	}
	
	return nil
}

// rotateLogIfNeeded creates a new WAL file if the current one is too large or doesn't exist
func (m *Manager) rotateLogIfNeeded() error {
	// Check if we need to rotate
	needsRotation := m.currentFile == nil || m.currentSize >= m.config.MaxWalSize
	if !needsRotation {
		return nil
	}
	
	// Close the current file if it exists
	if m.currentFile != nil {
		if err := m.writer.Flush(); err != nil {
			return err
		}
		if err := m.currentFile.Sync(); err != nil {
			return err
		}
		if err := m.currentFile.Close(); err != nil {
			return err
		}
	}
	
	// Create new WAL file
	m.currentFileNum++
	walFilePath := filepath.Join(m.config.Dir, fmt.Sprintf("%s_%06d.%s", 
		m.config.WalFilePrefix, m.currentFileNum, m.config.WalFileExt))
	
	file, err := os.OpenFile(walFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	
	m.currentFile = file
	m.writer = bufio.NewWriter(file)
	m.currentSize = 0
	
	return nil
}

// createCheckpoint creates a checkpoint
func (m *Manager) createCheckpoint() error {
	m.mu.Lock()
	
	// Flush any pending records
	if err := m.flushBatch(); err != nil {
		m.mu.Unlock()
		return err
	}
	
	// Create checkpoint record
	checkpointLSN := m.currentLSN
	prevCheckpointLSN := m.lastCheckpointLSN
	
	// Update last checkpoint LSN
	m.lastCheckpointLSN = checkpointLSN
	
	// Release lock before calling callback
	m.mu.Unlock()
	
	// Call checkpoint callback
	if m.checkpointCallback != nil {
		if err := m.checkpointCallback(checkpointLSN); err != nil {
			return err
		}
	}
	
	// Write checkpoint record to WAL
	record := NewCheckpointRecord(checkpointLSN, prevCheckpointLSN, 0) // keyCount will be set by callback
	if _, err := m.AppendRecord(record); err != nil {
		return err
	}
	
	// Flush checkpoint record
	return m.Flush()
}

// RecoverWAL recovers the database state from WAL files
func (m *Manager) RecoverWAL(recoveryHandler func(Record) error) error {
	// Find all WAL files
	pattern := filepath.Join(m.config.Dir, fmt.Sprintf("%s_*.%s", m.config.WalFilePrefix, m.config.WalFileExt))
	files, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("failed to list WAL files: %w", err)
	}
	
	if len(files) == 0 {
		return nil // No WAL files to recover
	}
	
	// Sort WAL files by number
	// (Simple implementation - in a real system, we'd sort by file number)
	
	// Process each WAL file in order
	for _, filePath := range files {
		if err := m.recoverWALFile(filePath, recoveryHandler); err != nil {
			return err
		}
	}
	
	return nil
}

// recoverWALFile recovers from a single WAL file
func (m *Manager) recoverWALFile(filePath string, recoveryHandler func(Record) error) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open WAL file: %w", err)
	}
	defer file.Close()
	
	reader := bufio.NewReader(file)
	
	for {
		// Read header
		header, err := ReadHeader(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read record header: %w", err)
		}
		
		// Read payload
		payload := make([]byte, header.Length)
		if _, err := io.ReadFull(reader, payload); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("failed to read record payload: %w", err)
		}
		
		// Verify checksum
		checksum := crc32.ChecksumIEEE(payload)
		if checksum != header.Checksum {
			return fmt.Errorf("checksum mismatch for record with LSN %d", header.LSN)
		}
		
		// Update current LSN if needed
		if header.LSN >= m.currentLSN {
			m.currentLSN = header.LSN + 1
		}
		
		// Update last checkpoint LSN if applicable
		if header.Type == RecordCheckpoint && header.LSN > m.lastCheckpointLSN {
			// Extract checkpoint LSN from payload
			if len(payload) >= 8 {
				checkpointLSN := LogSequenceNumber(binary.BigEndian.Uint64(payload[:8]))
				m.lastCheckpointLSN = checkpointLSN
			}
		}
		
		// Call recovery handler
		record := Record{
			Header:  header,
			Payload: payload,
		}
		if err := recoveryHandler(record); err != nil {
			return err
		}
	}
	
	return nil
}

// GetWALFilePath returns the path to the current WAL file
func (m *Manager) GetWALFilePath() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	return filepath.Join(m.config.Dir, fmt.Sprintf("%s_%06d.%s", 
		m.config.WalFilePrefix, m.currentFileNum, m.config.WalFileExt))
}

// PurgeOldWALFiles removes WAL files that are older than the last checkpoint
func (m *Manager) PurgeOldWALFiles() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if m.lastCheckpointLSN == 0 {
		return nil // No checkpoint yet, keep all files
	}
	
	// Find all WAL files
	pattern := filepath.Join(m.config.Dir, fmt.Sprintf("%s_*.%s", m.config.WalFilePrefix, m.config.WalFileExt))
	files, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("failed to list WAL files: %w", err)
	}
	
	// Find the file containing the last checkpoint
	checkpointFile := ""
	for _, file := range files {
		var fileNum int
		_, err := fmt.Sscanf(filepath.Base(file), fmt.Sprintf("%s_%%d.%s", m.config.WalFilePrefix, m.config.WalFileExt), &fileNum)
		if err != nil {
			continue
		}
		
		// Simple heuristic - real implementation would scan files to find checkpoint
		if fileNum < m.currentFileNum {
			checkpointFile = file
		}
	}
	
	if checkpointFile == "" {
		return nil // Can't find checkpoint file
	}
	
	// Delete files older than checkpoint file
	for _, file := range files {
		if file < checkpointFile && file != m.GetWALFilePath() {
			if err := os.Remove(file); err != nil {
				return fmt.Errorf("failed to delete old WAL file %s: %w", file, err)
			}
		}
	}
	
	return nil
} 