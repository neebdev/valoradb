package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/neebdev/valoradb/internal/config"
	"github.com/neebdev/valoradb/internal/parser"
	"github.com/neebdev/valoradb/internal/wal"
)

// Common error messages
var (
	ErrKeyNotFound        = errors.New("key not found")
	ErrTypeNotNumber      = errors.New("key is not a number")
	ErrDivideByZero       = errors.New("cannot divide by zero")
	ErrWalWriteFailed     = errors.New("failed to write to WAL")
	ErrWalSyncFailed      = errors.New("failed to sync WAL")
	ErrInvalidNumberValue = errors.New("invalid number value")
)

// Helper function to convert a string to a float64
func toFloat64(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}

// Helper function to format a float64 as a string
func formatFloat(val float64) string {
	return strconv.FormatFloat(val, 'f', -1, 64)
}

type Value struct {
	Data      string
	ValueType parser.ValueType
}

type Store struct {
	Data       map[string]Value
	KeyLocks   map[string]*sync.RWMutex // Fine-grained locks for each key
	GlobalLock sync.RWMutex             // Lock for operations on the store itself
	WalLock    sync.Mutex               // Lock for WAL operations 
	Wal        *wal.WAL
}

// Init initializes a new store
func NewStore(cfg *config.Config) (*Store, error) {
	// Create WAL options with settings from config
	walOpts := wal.DefaultWALOptions()
	
	// Use the directory from config
	walOpts.Dir = cfg.WAL.Directory
	walOpts.SegmentSize = cfg.WAL.SegmentSize
	
	fmt.Printf("Initializing database with WAL directory: %s\n", walOpts.Dir)
	
	// Ensure the WAL directory exists
	if err := os.MkdirAll(walOpts.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %v", err)
	}
	
	// Open the WAL
	walInstance, err := wal.OpenWAL(walOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %v", err)
	}
	
	return &Store{
		Data:       make(map[string]Value),
		KeyLocks:   make(map[string]*sync.RWMutex),
		Wal:        walInstance,
	}, nil
}

// getKeyLock gets or creates a lock for a specific key
func (s *Store) getKeyLock(key string) *sync.RWMutex {
	s.GlobalLock.RLock()
	lock, exists := s.KeyLocks[key]
	s.GlobalLock.RUnlock()
	
	if exists {
		return lock
	}
	
	// If lock doesn't exist, create it
	s.GlobalLock.Lock()
	defer s.GlobalLock.Unlock()
	
	// Check again in case another goroutine created it while we were waiting
	if lock, exists = s.KeyLocks[key]; exists {
		return lock
	}
	
	// Create new lock for this key
	lock = &sync.RWMutex{}
	s.KeyLocks[key] = lock
	return lock
}

// writeToWAL safely writes a record to the WAL
func (s *Store) writeToWAL(recordType wal.RecordType, key string, value *Value) error {
	s.WalLock.Lock()
	defer s.WalLock.Unlock()

	// Prepare payload data
	var payload []byte
	if value != nil {
		// Format: key + null terminator + value + null terminator + value type
		payload = []byte(key + "\x00" + value.Data + "\x00" + string(value.ValueType))
	} else {
		// Just the key for operations like GET, DEL, etc.
		payload = []byte(key)
	}
	
	// Create the record
	record := wal.NewRecord(uint8(recordType), 0, payload)
	
	// Append to WAL
	_, err := s.Wal.AppendRecord(record)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrWalWriteFailed, err)
	}
	
	return nil
}

func (s *Store) Set(key string, value Value) error {
	// First, log the operation to WAL
	if err := s.writeToWAL(wal.RecordSet, key, &value); err != nil {
		return err
	}

	// Get or create lock for this key
	keyLock := s.getKeyLock(key)
	
	// Lock just this key
	keyLock.Lock()
	defer keyLock.Unlock()
	
	// Global lock to access the map
	s.GlobalLock.Lock()
	defer s.GlobalLock.Unlock()

	s.Data[key] = value
	return nil
}

func (s *Store) Get(key string) (*Value, error) {
	// First, log the operation to WAL (read operations are also logged)
	if err := s.writeToWAL(wal.RecordNoop, key, nil); err != nil {
		return nil, err
	}

	// Get lock for this key
	keyLock := s.getKeyLock(key)
	
	// Use read lock for the key
	keyLock.RLock()
	defer keyLock.RUnlock()
	
	// Global read lock to access the map
	s.GlobalLock.RLock()
	defer s.GlobalLock.RUnlock()

	val, exists := s.Data[key]
	if !exists {
		return nil, ErrKeyNotFound
	}
	
	// Return a copy to avoid race conditions
	valCopy := val
	return &valCopy, nil
}

func (s *Store) Add(key string, value Value) error {
	// Lock for this operation
	s.GlobalLock.Lock()
	defer s.GlobalLock.Unlock()

	// Get the current value first
	existingVal, exists := s.Data[key]
	if !exists {
		return ErrKeyNotFound
	}

	if existingVal.ValueType != parser.TypeNumber || value.ValueType != parser.TypeNumber {
		return ErrTypeNotNumber
	}

	existingNum, err := toFloat64(existingVal.Data)
	if err != nil {
		return err
	}

	incomingNum, err := toFloat64(value.Data)
	if err != nil {
		return err
	}
	
	result := existingNum + incomingNum
	
	// Create the new value
	newValue := Value{
		Data:      formatFloat(result),
		ValueType: parser.TypeNumber,
	}

	// Log the operation to WAL
	if err := s.writeToWAL(wal.RecordAdd, key, &newValue); err != nil {
		return err
	}

	// Update the data
	s.Data[key] = newValue
	return nil
}

func (s *Store) Sub(key string, value Value) error {
	s.GlobalLock.Lock()
	defer s.GlobalLock.Unlock()

	existingVal, exists := s.Data[key]
	if !exists {
		return ErrKeyNotFound
	}

	if existingVal.ValueType != parser.TypeNumber || value.ValueType != parser.TypeNumber {
		return ErrTypeNotNumber
	}

	existingNum, err := toFloat64(existingVal.Data)
	if err != nil {
		return err
	}

	incomingNum, err := toFloat64(value.Data)
	if err != nil {
		return err
	}
	
	result := existingNum - incomingNum
	
	// Create the new value
	newValue := Value{
		Data:      formatFloat(result),
		ValueType: parser.TypeNumber,
	}

	// Log the operation to WAL
	if err := s.writeToWAL(wal.RecordSub, key, &newValue); err != nil {
		return err
	}

	// Update the data
	s.Data[key] = newValue
	return nil
}

func (s *Store) Mul(key string, value Value) error {
	s.GlobalLock.Lock()
	defer s.GlobalLock.Unlock()

	existingVal, exists := s.Data[key]
	if !exists {
		return ErrKeyNotFound
	}

	if existingVal.ValueType != parser.TypeNumber || value.ValueType != parser.TypeNumber {
		return ErrTypeNotNumber
	}

	existingNum, err := toFloat64(existingVal.Data)
	if err != nil {
		return err
	}

	incomingNum, err := toFloat64(value.Data)
	if err != nil {
		return err
	}
	
	result := existingNum * incomingNum
	
	// Create the new value
	newValue := Value{
		Data:      formatFloat(result),
		ValueType: parser.TypeNumber,
	}

	// Log the operation to WAL
	if err := s.writeToWAL(wal.RecordMul, key, &newValue); err != nil {
		return err
	}

	// Update the data
	s.Data[key] = newValue
	return nil
}

func (s *Store) Div(key string, value Value) error {
	s.GlobalLock.Lock()
	defer s.GlobalLock.Unlock()

	existingVal, exists := s.Data[key]
	if !exists {
		return ErrKeyNotFound
	}

	if existingVal.ValueType != parser.TypeNumber || value.ValueType != parser.TypeNumber {
		return ErrTypeNotNumber
	}

	existingNum, err := toFloat64(existingVal.Data)
	if err != nil {
		return err
	}

	incomingNum, err := toFloat64(value.Data)
	if err != nil {
		return err
	}
	
	if incomingNum == 0 {
		return ErrDivideByZero
	}
	
	result := existingNum / incomingNum
	
	// Create the new value
	newValue := Value{
		Data:      formatFloat(result),
		ValueType: parser.TypeNumber,
	}

	// Log the operation to WAL
	if err := s.writeToWAL(wal.RecordDiv, key, &newValue); err != nil {
		return err
	}

	// Update the data
	s.Data[key] = newValue
	return nil
}

func (s *Store) Keys(pattern string) ([]string, error) {
	s.GlobalLock.RLock()
	defer s.GlobalLock.RUnlock()

	// Log the operation (non-modifying operations still get logged for completeness)
	if err := s.writeToWAL(wal.RecordNoop, "KEYS:"+pattern, nil); err != nil {
		return nil, err
	}

	var matches []string
	for key := range s.Data {
		if ok, _ := filepath.Match(pattern, key); ok {
			matches = append(matches, key)
		}
	}

	return matches, nil
}

func (s *Store) Del(key string) error {
	s.GlobalLock.Lock()
	defer s.GlobalLock.Unlock()

	// Check if key exists first
	_, exists := s.Data[key]
	if !exists {
		return fmt.Errorf("%w: %s", ErrKeyNotFound, key)
	}

	// Log the delete operation to WAL
	if err := s.writeToWAL(wal.RecordDel, key, nil); err != nil {
		return err
	}

	// Delete the key
	delete(s.Data, key)
	return nil
}

func (s *Store) Exists(key string) (bool, error) {
	s.GlobalLock.RLock()
	defer s.GlobalLock.RUnlock()

	// Log the operation (read operations still get logged)
	if err := s.writeToWAL(wal.RecordNoop, "EXISTS:"+key, nil); err != nil {
		return false, err
	}
	
	_, exists := s.Data[key]
	return exists, nil
}

func (s *Store) Type(key string) (parser.ValueType, error) {
	s.GlobalLock.RLock()
	defer s.GlobalLock.RUnlock()

	// Log the operation (read operations still get logged)
	if err := s.writeToWAL(wal.RecordNoop, "TYPE:"+key, nil); err != nil {
		return "", err
	}

	val, exists := s.Data[key]
	if !exists {
		return "", fmt.Errorf("%w: %s", ErrKeyNotFound, key)
	}

	return val.ValueType, nil
}

// RecoverFromWAL rebuilds the database state from the WAL
func (s *Store) RecoverFromWAL() error {
	// Acquire global lock during recovery
	s.GlobalLock.Lock()
	defer s.GlobalLock.Unlock()
	
	// Create a WAL reader
	reader, err := s.Wal.Reader()
	if err != nil {
		// Check if this is because the WAL file doesn't exist yet
		if os.IsNotExist(err) {
			fmt.Println("No WAL file found. Starting with fresh database.")
			return nil
		}
		return fmt.Errorf("failed to create WAL reader: %v", err)
	}
	defer reader.Close()
	
	// Clear existing data (start with clean state)
	s.Data = make(map[string]Value)
	
	recordCount := 0
	recovered := 0
	
	// Track if we've seen a CLEAR record
	var lastClearRecord int = -1
	
	// First pass: scan through all records to find the last CLEAR record
	records := make([]wal.Record, 0)
	
	for {
		record, err := reader.ReadRecord()
		if err != nil {
			// EOF is expected when we reach the end of the WAL
			if err.Error() == "EOF" {
				break
			}
			
			// Handle corrupt WAL records by stopping recovery but not failing
			if strings.Contains(err.Error(), "invalid record magic number") || 
			   strings.Contains(err.Error(), "corrupted WAL record") {
				fmt.Printf("Corrupt WAL detected: %v\n", err)
				fmt.Println("Recovery incomplete, some transactions may be lost.")
				break
			}
			
			return fmt.Errorf("error reading WAL record: %v", err)
		}
		
		records = append(records, *record)
		
		// Check if this is a CLEAR record
		if wal.RecordType(record.Header.Type) == wal.RecordClear {
			lastClearRecord = len(records) - 1
		}
	}
	
	// If we found a CLEAR record, only process records after it
	startIndex := 0
	if lastClearRecord >= 0 {
		startIndex = lastClearRecord + 1
	}
	
	// Second pass: apply all records after the last CLEAR
	for i := startIndex; i < len(records); i++ {
		record := records[i]
		recordCount++
		
		// Skip NOOP records (used for read operations)
		if record.Header.Type == uint8(wal.RecordNoop) {
			continue
		}
		
		// Parse the payload
		payload := string(record.Payload)
		
		// Process based on record type
		switch wal.RecordType(record.Header.Type) {
		case wal.RecordSet:
			// Format: key + null terminator + value + null terminator + value type
			parts := strings.Split(payload, "\x00")
			if len(parts) >= 3 {
				key := parts[0]
				value := parts[1]
				valueType := parser.ValueType(parts[2])
				
				s.Data[key] = Value{
					Data:      value,
					ValueType: valueType,
				}
				recovered++
			}
			
		case wal.RecordDel:
			// Just a key in the payload
			key := payload
			delete(s.Data, key)
			
		case wal.RecordAdd, wal.RecordSub, wal.RecordMul, wal.RecordDiv:
			// Format: key + null terminator + result value + null terminator + value type
			parts := strings.Split(payload, "\x00")
			if len(parts) >= 3 {
				key := parts[0]
				value := parts[1]
				valueType := parser.ValueType(parts[2])
				
				s.Data[key] = Value{
					Data:      value,
					ValueType: valueType,
				}
				recovered++
			}
		}
	}
	
	if recordCount > 0 {
		fmt.Printf("Recovery complete: processed %d WAL records, recovered %d keys\n", 
			recordCount, len(s.Data))
	} else {
		fmt.Println("No records found in WAL. Starting with empty database.")
	}
	
	return nil
}

// Clear removes all data from the store and creates a new WAL file
func (s *Store) Clear() error {
	// Acquire global lock to ensure exclusive access during clear
	s.GlobalLock.Lock()
	defer s.GlobalLock.Unlock()

	// Close the existing WAL
	if err := s.Wal.Close(); err != nil {
		fmt.Printf("Warning: failed to close WAL during clear: %v\n", err)
		// Continue anyway
	}

	// Get the current WAL directory
	walDir := s.Wal.GetDirectory()

	// Create a new WAL file with the same options
	walOpts := wal.DefaultWALOptions()
	walOpts.Dir = walDir
	
	newWal, err := wal.OpenWAL(walOpts)
	if err != nil {
		return fmt.Errorf("failed to create new WAL during clear: %w", err)
	}

	// Set the new WAL and clear all data
	s.Wal = newWal
	s.Data = make(map[string]Value)
	
	// We don't need to clear key locks as they're created on demand
	
	// Now that we have a new WAL, log the CLEAR operation
	if err := s.writeToWAL(wal.RecordClear, "CLEAR", nil); err != nil {
		return fmt.Errorf("failed to write CLEAR record to WAL: %w", err)
	}
	
	fmt.Println("Database cleared successfully")
	return nil
}