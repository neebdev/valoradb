package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrInvalidWAL is returned when the WAL file format is invalid
	ErrInvalidWAL       = errors.New("invalid WAL")
	// ErrCorruptedRecord is returned when a record's checksum verification fails
	ErrCorruptedRecord  = errors.New("corrupted WAL record")
	// ErrWALClosed is returned when an operation is attempted on a closed WAL
	ErrWALClosed        = errors.New("WAL is closed")
)

// WAL (Write-Ahead Log) manages the durability of database operations
type WAL struct {
	dir           string
	currentFile   *os.File
	currentLSN    atomic.Uint64
	mu            sync.RWMutex
	closed        bool
	segmentSize   int64
	fileSize      int64
	flushInterval time.Duration
	stopCh        chan struct{}
	writeQueue    chan Record
	syncWg        sync.WaitGroup
	lastTxID      uint64
}

// Options contains configuration options for the WAL
type Options struct {
	Dir           string // Base directory for WAL files
	DBName        string // Database name for organizing WAL files
	SegmentSize   int64  // Max size of each WAL segment in bytes
	FlushInterval time.Duration
	QueueSize     int
}

// DefaultWALOptions returns default WAL options
func DefaultWALOptions() Options {
	return Options{
		Dir:           "data",
		DBName:        "default",
		SegmentSize:   16 * 1024 * 1024, // 16MB segments
		FlushInterval: 100 * time.Millisecond,
		QueueSize:     1000,
	}
}

// OpenWAL opens or creates a WAL at the specified directory
func OpenWAL(opts Options) (*WAL, error) {
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = DefaultWALOptions().SegmentSize
	}
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = DefaultWALOptions().FlushInterval
	}
	if opts.QueueSize <= 0 {
		opts.QueueSize = DefaultWALOptions().QueueSize
	}
	if opts.DBName == "" {
		opts.DBName = DefaultWALOptions().DBName
	}
	
	// Create the base directory if it doesn't exist
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, err
	}
	
	// Create the WAL directory structure: data/<dbname>/wal
	walDir := filepath.Join(opts.Dir, opts.DBName, "wal")
	if err := os.MkdirAll(walDir, 0755); err != nil {
		return nil, err
	}
	
	// Create date-based subdirectory (YYYY-MM)
	now := time.Now()
	dateDir := filepath.Join(walDir, fmt.Sprintf("%04d-%02d", now.Year(), now.Month()))
	if err := os.MkdirAll(dateDir, 0755); err != nil {
		return nil, err
	}
	
	w := &WAL{
		dir:           dateDir,
		segmentSize:   opts.SegmentSize,
		flushInterval: opts.FlushInterval,
		stopCh:        make(chan struct{}),
		writeQueue:    make(chan Record, opts.QueueSize),
	}
	
	// Find the max LSN from existing files
	maxLSN, err := w.recoverMaxLSN()
	if err != nil {
		return nil, err
	}
	w.currentLSN.Store(maxLSN)
	
	// Create or open the current WAL segment
	if err := w.openCurrentSegment(); err != nil {
		return nil, err
	}
	
	// Start background processor
	w.syncWg.Add(1)
	go w.backgroundWriter()
	
	return w, nil
}

// recoverMaxLSN reads existing WAL files to find the maximum LSN
func (w *WAL) recoverMaxLSN() (uint64, error) {
	// Get all segment files in the current month directory
	files, err := filepath.Glob(filepath.Join(w.dir, "wal-*.log"))
	if err != nil {
		return 0, err
	}
	
	if len(files) == 0 {
		// If no files in current month, check previous months
		baseDir := filepath.Dir(w.dir) // Go up one level to wal directory
		dateDirs, err := findDateDirs(baseDir)
		if err != nil {
			return 0, nil // Assume no existing WAL files on error
		}
		
		// Sort date directories in descending order
		sort.Sort(sort.Reverse(sort.StringSlice(dateDirs)))
		
		for _, dateDir := range dateDirs {
			if dateDir == filepath.Base(w.dir) {
				continue // Skip current month (already checked)
			}
			
			prevFiles, err := filepath.Glob(filepath.Join(baseDir, dateDir, "wal-*.log"))
			if err != nil {
				continue
			}
			
			if len(prevFiles) > 0 {
				// Found files in a previous month, process them
				return findMaxLSNFromFiles(prevFiles)
			}
		}
		
		return 0, nil // No existing WAL files
	}
	
	return findMaxLSNFromFiles(files)
}

// findDateDirs returns all date-formatted directories in the given base directory
func findDateDirs(baseDir string) ([]string, error) {
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return nil, err
	}
	
	var dateDirs []string
	dateRegex := regexp.MustCompile(`^\d{4}-\d{2}$`)
	
	for _, entry := range entries {
		if entry.IsDir() && dateRegex.MatchString(entry.Name()) {
			dateDirs = append(dateDirs, entry.Name())
		}
	}
	
	return dateDirs, nil
}

// findMaxLSNFromFiles examines a list of WAL files and returns the maximum LSN
func findMaxLSNFromFiles(files []string) (uint64, error) {
	if len(files) == 0 {
		return 0, nil
	}
	
	var maxLSN uint64 = 0
	
	// Extract LSN from filenames and find the maximum
	for _, file := range files {
		baseName := filepath.Base(file)
		// Format: wal-<LSN>.log
		if !strings.HasPrefix(baseName, "wal-") || !strings.HasSuffix(baseName, ".log") {
			continue
		}
		
		lsnStr := strings.TrimPrefix(baseName, "wal-")
		lsnStr = strings.TrimSuffix(lsnStr, ".log")
		
		lsn, err := strconv.ParseUint(lsnStr, 10, 64)
		if err != nil {
			continue
		}
		
		if lsn > maxLSN {
			maxLSN = lsn
		}
	}
	
	return maxLSN, nil
}

// extractLSNFromFilename extracts the LSN from a WAL segment filename (format: wal-<LSN>.log)
func extractLSNFromFilename(filename string) uint64 {
	// Extract the base filename
	base := filepath.Base(filename)
	
	// Remove "wal-" prefix and ".log" suffix
	lsnStr := strings.TrimPrefix(base, "wal-")
	lsnStr = strings.TrimSuffix(lsnStr, ".log")
	
	// Parse the LSN
	lsn, err := strconv.ParseUint(lsnStr, 10, 64)
	if err != nil {
		return 0
	}
	
	return lsn
}

// openCurrentSegment creates or opens the current WAL segment file
func (w *WAL) openCurrentSegment() error {
	currentLSN := w.currentLSN.Load()
	
	// Generate filename based on the current LSN
	filename := filepath.Join(w.dir, fmt.Sprintf("wal-%020d.log", currentLSN))
	
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	
	// Get current file size
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return err
	}
	
	w.currentFile = file
	w.fileSize = info.Size()
	
	return nil
}

// nextSegment closes the current segment and opens a new one
func (w *WAL) nextSegment() error {
	if w.currentFile != nil {
		if err := w.currentFile.Sync(); err != nil {
			return err
		}
		if err := w.currentFile.Close(); err != nil {
			return err
		}
	}
	
	return w.openCurrentSegment()
}

// NextLSN generates a new LSN
func (w *WAL) NextLSN() LogSequenceNumber {
	return LogSequenceNumber(w.currentLSN.Add(1))
}

// AppendRecord adds a record to the WAL
func (w *WAL) AppendRecord(record Record) (LogSequenceNumber, error) {
	w.mu.RLock()
	if w.closed {
		w.mu.RUnlock()
		return 0, ErrWALClosed
	}
	w.mu.RUnlock()
	
	// Assign a new LSN if one hasn't been assigned
	if record.Header.LSN == 0 {
		record.Header.LSN = w.NextLSN()
	}
	
	// Queue the record for writing
	select {
	case w.writeQueue <- record:
		return record.Header.LSN, nil
	case <-w.stopCh:
		return 0, ErrWALClosed
	}
}

// backgroundWriter processes queued records and writes them to disk
func (w *WAL) backgroundWriter() {
	defer w.syncWg.Done()
	
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()
	
	batch := make([]Record, 0, 100)
	
	for {
		select {
		case record := <-w.writeQueue:
			batch = append(batch, record)
			
			// Process more records if available without blocking
			drainLoop:
			for len(batch) < cap(batch) {
				select {
				case record := <-w.writeQueue:
					batch = append(batch, record)
				default:
					break drainLoop
				}
			}
			
			if err := w.writeBatch(batch); err != nil {
				// TODO: Better error handling
				fmt.Printf("WAL write error: %v\n", err)
			}
			
			// Reset batch
			batch = batch[:0]
			
		case <-ticker.C:
			// Sync to disk periodically
			w.mu.RLock()
			if w.currentFile != nil && !w.closed {
				_ = w.currentFile.Sync()
			}
			w.mu.RUnlock()
			
		case <-w.stopCh:
			// Process any remaining records
			close(w.writeQueue)
			for record := range w.writeQueue {
				batch = append(batch, record)
			}
			
			if len(batch) > 0 {
				if err := w.writeBatch(batch); err != nil {
					fmt.Printf("WAL final write error: %v\n", err)
				}
			}
			
			w.mu.Lock()
			if w.currentFile != nil {
				_ = w.currentFile.Sync()
				_ = w.currentFile.Close()
				w.currentFile = nil
			}
			w.mu.Unlock()
			
			return
		}
	}
}

// writeBatch writes a batch of records to the WAL file
func (w *WAL) writeBatch(records []Record) error {
	if len(records) == 0 {
		return nil
	}
	
	w.mu.Lock()
	defer w.mu.Unlock()
	
	if w.closed {
		return ErrWALClosed
	}
	
	for i := range records {
		record := &records[i]
		
		// Calculate checksum if not already done
		if record.Header.Checksum == 0 {
			record.Header.Checksum = crc32.ChecksumIEEE(record.Payload)
		}
		
		// Set timestamp if not already set
		if record.Header.Timestamp == 0 {
			record.Header.Timestamp = time.Now().UnixNano()
		}
		
		// Check if we need to rotate to a new segment
		recordSize := int64(HeaderSize + len(record.Payload))
		if w.fileSize+recordSize > w.segmentSize {
			if err := w.nextSegment(); err != nil {
				return err
			}
			w.fileSize = 0
		}
		
		// Write header
		if err := WriteHeader(w.currentFile, record.Header); err != nil {
			return err
		}
		
		// Write payload
		n, err := w.currentFile.Write(record.Payload)
		if err != nil {
			return err
		}
		if n != len(record.Payload) {
			return io.ErrShortWrite
		}
		
		w.fileSize += recordSize
	}
	
	return nil
}

// Sync flushes all pending writes to disk
func (w *WAL) Sync() error {
	w.mu.RLock()
	defer w.mu.RUnlock()
	
	if w.closed {
		return ErrWALClosed
	}
	
	if w.currentFile != nil {
		return w.currentFile.Sync()
	}
	
	return nil
}

// Close closes the WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	w.mu.Unlock()
	
	// Signal background writer to stop
	close(w.stopCh)
	
	// Wait for background writer to finish
	w.syncWg.Wait()
	
	return nil
}

// Checkpoint creates a checkpoint record that can be used for recovery
func (w *WAL) Checkpoint(keyCount uint32) (LogSequenceNumber, error) {
	prevCheckLSN := LogSequenceNumber(w.currentLSN.Load())
	checkpointLSN := w.NextLSN()
	
	record := NewCheckpointRecord(checkpointLSN, prevCheckLSN, uint64(keyCount))
	
	_, err := w.AppendRecord(record)
	if err != nil {
		return 0, err
	}
	
	// Force sync to disk
	if err := w.Sync(); err != nil {
		return 0, err
	}
	
	return checkpointLSN, nil
}

// Reader returns a reader for WAL recovery
func (w *WAL) Reader() (*Reader, error) {
	return NewWALReader(w.dir)
}

// Reader is used to read WAL records for recovery
type Reader struct {
	dir       string
	files     []string
	currentFd *os.File
	fileIndex int
}

// NewWALReader creates a new WAL reader
func NewWALReader(dir string) (*Reader, error) {
	// Get the base WAL directory
	baseDir := filepath.Dir(dir) // Go up one level to the wal directory
	
	// Find all date directories
	dateDirs, err := findDateDirs(baseDir)
	if err != nil {
		return nil, err
	}
	
	// Sort date directories in ascending order (oldest first)
	sort.Strings(dateDirs)
	
	// Get all WAL files from all date directories
	var allFiles []string
	for _, dateDir := range dateDirs {
		dateWalDir := filepath.Join(baseDir, dateDir)
		files, err := filepath.Glob(filepath.Join(dateWalDir, "wal-*.log"))
		if err != nil {
			continue
		}
		allFiles = append(allFiles, files...)
	}
	
	if len(allFiles) == 0 {
		return nil, ErrInvalidWAL
	}
	
	// Sort files by LSN embedded in filename
	sort.Slice(allFiles, func(i, j int) bool {
		iLSN := extractLSNFromFilename(allFiles[i])
		jLSN := extractLSNFromFilename(allFiles[j])
		return iLSN < jLSN
	})
	
	reader := &Reader{
		dir:       dir,
		files:     allFiles,
		fileIndex: 0,
	}
	
	// Open the first file
	if err := reader.openCurrentFile(); err != nil {
		return nil, err
	}
	
	return reader, nil
}

// openCurrentFile opens the current WAL segment file
func (r *Reader) openCurrentFile() error {
	if r.fileIndex >= len(r.files) {
		return io.EOF
	}
	
	if r.currentFd != nil {
		r.currentFd.Close()
	}
	
	file, err := os.Open(r.files[r.fileIndex])
	if err != nil {
		return err
	}
	
	r.currentFd = file
	return nil
}

// ReadRecord reads the next record from the WAL
func (r *Reader) ReadRecord() (*Record, error) {
	for {
		if r.currentFd == nil {
			if err := r.openCurrentFile(); err != nil {
				return nil, err
			}
		}
		
		// Read record header
		header, err := ReadHeader(r.currentFd)
		if err != nil {
			if err == io.EOF {
				// Move to next file
				r.fileIndex++
				if r.fileIndex >= len(r.files) {
					return nil, io.EOF
				}
				if err := r.openCurrentFile(); err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		
		// Read payload
		payload := make([]byte, header.Length)
		n, err := io.ReadFull(r.currentFd, payload)
		if err != nil {
			return nil, err
		}
		if uint32(n) != header.Length {
			return nil, io.ErrUnexpectedEOF
		}
		
		// Verify checksum
		checksum := crc32.ChecksumIEEE(payload)
		if checksum != header.Checksum {
			return nil, ErrCorruptedRecord
		}
		
		record := &Record{
			Header:  header,
			Payload: payload,
		}
		
		return record, nil
	}
}

// Close closes the WAL reader
func (r *Reader) Close() error {
	if r.currentFd != nil {
		return r.currentFd.Close()
	}
	return nil
}

// TransactionID represents a unique identifier for a transaction
type TransactionID uint64

// TransactionStatus represents the status of a transaction
type TransactionStatus uint8

const (
	// TransactionStatusUnknown is the default status
	TransactionStatusUnknown TransactionStatus = iota
	// TransactionStatusActive indicates an active transaction
	TransactionStatusActive
	// TransactionStatusCommitted indicates a committed transaction
	TransactionStatusCommitted
	// TransactionStatusAborted indicates an aborted transaction
	TransactionStatusAborted
)

// RecordType extends to include transaction-related records
const (
	// RecordTypeTransactionBegin indicates the start of a transaction
	RecordTypeTransactionBegin RecordType = 0x20
	// RecordTypeTransactionCommit indicates a transaction commit
	RecordTypeTransactionCommit RecordType = 0x21
	// RecordTypeTransactionAbort indicates a transaction abort
	RecordTypeTransactionAbort RecordType = 0x22
	// RecordTypeBatchBegin indicates the start of a batch
	RecordTypeBatchBegin RecordType = 0x28
	// RecordTypeBatchEnd indicates the end of a batch
	RecordTypeBatchEnd RecordType = 0x29
)

// TransactionRecord represents a transaction-related record in the WAL
type TransactionRecord struct {
	LSN           LogSequenceNumber
	TransactionID TransactionID
	Status        TransactionStatus
	Type          RecordType
	Timestamp     time.Time
}

// NewTransactionRecord creates a new transaction record
func NewTransactionRecord(lsn LogSequenceNumber, txID TransactionID, recordType RecordType) Record {
	// Create a record with the transaction information in the payload
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, uint64(txID))
	
	// Create the record using the NewRecord function
	record := NewRecord(uint8(recordType), uint64(txID), payload)
	
	// Set the LSN as it's not set by NewRecord
	record.Header.LSN = lsn
	
	return record
}

// BatchRecord represents a group of records to be written atomically
type BatchRecord struct {
	records []Record
	size    int
}

// NewBatchRecord creates a new batch record
func NewBatchRecord() *BatchRecord {
	return &BatchRecord{
		records: make([]Record, 0),
		size:    0,
	}
}

// AddRecord adds a record to the batch
func (b *BatchRecord) AddRecord(record Record) {
	b.records = append(b.records, record)
	b.size += int(record.Header.Length) + HeaderSize
}

// Size returns the total size of all records in the batch
func (b *BatchRecord) Size() int {
	return b.size
}

// Count returns the number of records in the batch
func (b *BatchRecord) Count() int {
	return len(b.records)
}

// BeginTransaction starts a new transaction in the WAL
func (w *WAL) BeginTransaction() (TransactionID, LogSequenceNumber, error) {
	if w.closed {
		return 0, 0, ErrWALClosed
	}
	
	txID := TransactionID(atomic.AddUint64(&w.lastTxID, 1))
	lsn := w.NextLSN()
	
	record := NewTransactionRecord(lsn, txID, RecordTypeTransactionBegin)
	_, err := w.AppendRecord(record)
	if err != nil {
		return 0, 0, err
	}
	
	return txID, lsn, nil
}

// CommitTransaction commits a transaction
func (w *WAL) CommitTransaction(txID TransactionID) (LogSequenceNumber, error) {
	if w.closed {
		return 0, ErrWALClosed
	}
	
	lsn := w.NextLSN()
	record := NewTransactionRecord(lsn, txID, RecordTypeTransactionCommit)
	_, err := w.AppendRecord(record)
	if err != nil {
		return 0, err
	}
	
	return lsn, nil
}

// AbortTransaction aborts a transaction
func (w *WAL) AbortTransaction(txID TransactionID) (LogSequenceNumber, error) {
	if w.closed {
		return 0, ErrWALClosed
	}
	
	lsn := w.NextLSN()
	record := NewTransactionRecord(lsn, txID, RecordTypeTransactionAbort)
	_, err := w.AppendRecord(record)
	if err != nil {
		return 0, err
	}
	
	return lsn, nil
}

// WriteBatch writes a batch of records atomically
func (w *WAL) WriteBatch(batch *BatchRecord) (LogSequenceNumber, error) {
	if w.closed {
		return 0, ErrWALClosed
	}
	
	w.mu.Lock()
	defer w.mu.Unlock()
	
	startLSN := w.NextLSN()
	
	// Begin batch record
	beginRecord := NewRecord(uint8(RecordTypeBatchBegin), 0, []byte{})
	beginRecord.Header.LSN = startLSN
	
	if _, err := w.AppendRecord(beginRecord); err != nil {
		return 0, err
	}
	
	// Write all records in the batch
	for _, record := range batch.records {
		if _, err := w.AppendRecord(record); err != nil {
			return 0, err
		}
	}
	
	// End batch record
	endLSN := w.NextLSN()
	endRecord := NewRecord(uint8(RecordTypeBatchEnd), 0, []byte{})
	endRecord.Header.LSN = endLSN
	
	if _, err := w.AppendRecord(endRecord); err != nil {
		return 0, err
	}
	
	// Force sync for batch commits
	if err := w.Sync(); err != nil {
		return 0, err
	}
	
	return endLSN, nil
}

// RotateLog rotates the current log file by creating a new segment
func (w *WAL) RotateLog() error {
	if w.closed {
		return ErrWALClosed
	}
	
	w.mu.Lock()
	defer w.mu.Unlock()
	
	// First sync the current segment
	if err := w.Sync(); err != nil {
		return err
	}
	
	// Close current segment
	if err := w.currentFile.Close(); err != nil {
		return err
	}
	
	// Check if we need to rotate to a new month directory
	now := time.Now()
	currentMonthDir := filepath.Base(w.dir)
	expectedMonthDir := fmt.Sprintf("%04d-%02d", now.Year(), now.Month())
	
	if currentMonthDir != expectedMonthDir {
		// Create a new month directory
		baseDir := filepath.Dir(w.dir) // Get the wal directory
		newDir := filepath.Join(baseDir, expectedMonthDir)
		if err := os.MkdirAll(newDir, 0755); err != nil {
			return err
		}
		w.dir = newDir
	}
	
	// Create a new segment
	segmentID := w.currentLSN.Add(1)
	segmentPath := filepath.Join(w.dir, fmt.Sprintf("wal-%020d.log", segmentID))
	segment, err := os.OpenFile(segmentPath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	
	w.currentFile = segment
	w.fileSize = 0
	
	return nil
}

// TruncateBefore removes all segments with IDs less than the given segment ID
func (w *WAL) TruncateBefore(segmentID uint64) error {
	if w.closed {
		return ErrWALClosed
	}
	
	// Cannot truncate the current segment
	if segmentID >= w.currentLSN.Load() {
		return fmt.Errorf("cannot truncate current or future segments")
	}
	
	// Get the base WAL directory
	baseDir := filepath.Dir(w.dir) // Go up one level to the wal directory
	
	// Find all date directories
	dateDirs, err := findDateDirs(baseDir)
	if err != nil {
		return err
	}
	
	for _, dateDir := range dateDirs {
		dir := filepath.Join(baseDir, dateDir)
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		
		for _, entry := range entries {
			if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".log") && strings.HasPrefix(entry.Name(), "wal-") {
				// Extract segment ID from filename
				baseName := entry.Name()
				lsnStr := strings.TrimPrefix(baseName, "wal-")
				lsnStr = strings.TrimSuffix(lsnStr, ".log")
				
				id, err := strconv.ParseUint(lsnStr, 10, 64)
				if err != nil {
					continue
				}
				
				// If segment ID is less than the given ID, remove the file
				if id < segmentID {
					filePath := filepath.Join(dir, entry.Name())
					if err := os.Remove(filePath); err != nil {
						return err
					}
				}
			}
		}
		
		// Check if the directory is now empty and remove it if it's not the current month
		if dateDir != filepath.Base(w.dir) {
			entries, err := os.ReadDir(filepath.Join(baseDir, dateDir))
			if err != nil {
				continue
			}
			
			if len(entries) == 0 {
				// Directory is empty, remove it
				if err := os.Remove(filepath.Join(baseDir, dateDir)); err != nil {
					// Non-fatal error, just continue
					continue
				}
			}
		}
	}
	
	return nil
}

// LastTransactionID returns the last assigned transaction ID
func (w *WAL) LastTransactionID() TransactionID {
	return TransactionID(w.lastTxID)
} 