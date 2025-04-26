package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/neebdev/valoradb/internal/parser"
)

// Common error messages
var (
	ErrKeyNotFound        = errors.New("key not found")
	ErrTypeNotNumber      = errors.New("operation supports only number types")
	ErrDivideByZero       = errors.New("division by zero")
	ErrWalWriteFailed     = errors.New("failed to write to WAL")
	ErrWalSyncFailed      = errors.New("failed to sync WAL")
	ErrInvalidNumberValue = errors.New("invalid number value")
)

// Helper function to convert string to float64
func toFloat64(s string) (float64, error) {
	val, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: %s", ErrInvalidNumberValue, s)
	}
	return val, nil
}

// Helper function to format float result
func formatFloat(val float64) string {
	return fmt.Sprintf("%f", val)
}

// Helper function to write to WAL
func writeToWal(file *os.File, format string, args ...interface{}) error {
	logLine := fmt.Sprintf(format, args...)
	if _, err := file.WriteString(logLine); err != nil {
		return fmt.Errorf("%w: %v", ErrWalWriteFailed, err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("%w: %v", ErrWalSyncFailed, err)
	}
	return nil
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
	Wal        *os.File
}

// Init initializes a new store
func NewStore(walPath string) (*Store, error) {
	walFile, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create/open WAL file: %v", err)
	}
	
	return &Store{
		Data:       make(map[string]Value),
		KeyLocks:   make(map[string]*sync.RWMutex),
		Wal:        walFile,
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

// writeToWalSafe safely writes to the WAL file with proper locking
func (s *Store) writeToWalSafe(format string, args ...interface{}) error {
	s.WalLock.Lock()
	defer s.WalLock.Unlock()
	
	return writeToWal(s.Wal, format, args...)
}

func (s *Store) Set(key string, value Value) error {
	// First, log the operation
	if err := s.writeToWalSafe("SET %s %s TYPE %s\n", key, value.Data, value.ValueType); err != nil {
		return err
	}

	// Get or create lock for this key
	keyLock := s.getKeyLock(key)
	
	// Lock just this key
	keyLock.Lock()
	defer keyLock.Unlock()
	
	// Global read lock to access the map
	s.GlobalLock.Lock()
	defer s.GlobalLock.Unlock()

	s.Data[key] = value
	return nil
}

func (s *Store) Get(key string) (*Value, error) {
	// First, log the operation
	if err := s.writeToWalSafe("GET %s\n", key); err != nil {
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
	s.GlobalLock.Lock()
	defer s.GlobalLock.Unlock()

	if err := s.writeToWalSafe("ADD %s %s TYPE %s\n", key, value.Data, value.ValueType); err != nil {
		return err
	}

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

	s.Data[key] = Value{
		Data:      formatFloat(result),
		ValueType: parser.TypeNumber,
	}

	return nil
}

func (s *Store) Sub(key string, value Value) error {
	s.GlobalLock.Lock()
	defer s.GlobalLock.Unlock()

	if err := s.writeToWalSafe("SUB %s %s TYPE %s\n", key, value.Data, value.ValueType); err != nil {
		return err
	}

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

	s.Data[key] = Value{
		Data:      formatFloat(result),
		ValueType: parser.TypeNumber,
	}

	return nil
}

func (s *Store) Mul(key string, value Value) error {
	s.GlobalLock.Lock()
	defer s.GlobalLock.Unlock()

	if err := s.writeToWalSafe("MUL %s %s TYPE %s\n", key, value.Data, value.ValueType); err != nil {
		return err
	}

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

	s.Data[key] = Value{
		Data:      formatFloat(result),
		ValueType: parser.TypeNumber,
	}

	return nil
}

func (s *Store) Div(key string, value Value) error {
	s.GlobalLock.Lock()
	defer s.GlobalLock.Unlock()

	if err := s.writeToWalSafe("DIV %s %s TYPE %s\n", key, value.Data, value.ValueType); err != nil {
		return err
	}

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

	s.Data[key] = Value{
		Data:      formatFloat(result),
		ValueType: parser.TypeNumber,
	}

	return nil
}

func (s *Store) Keys(pattern string) ([]string, error) {
	s.GlobalLock.RLock()
	defer s.GlobalLock.RUnlock()

	if err := s.writeToWalSafe("KEYS %s\n", pattern); err != nil {
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

	if err := s.writeToWalSafe("DEL %s\n", key); err != nil {
		return err
	}

	_, exists := s.Data[key]
	if !exists {
		return fmt.Errorf("%w: %s", ErrKeyNotFound, key)
	}

	delete(s.Data, key)
	return nil
}

func (s *Store) Exists(key string) (bool, error) {
	s.GlobalLock.RLock()
	defer s.GlobalLock.RUnlock()

	if err := s.writeToWalSafe("EXISTS %s\n", key); err != nil {
		return false, err
	}
	
	_, exists := s.Data[key]
	return exists, nil
}

func (s *Store) Type(key string) (parser.ValueType, error) {
	s.GlobalLock.RLock()
	defer s.GlobalLock.RUnlock()

	if err := s.writeToWalSafe("TYPE %s\n", key); err != nil {
		return "", err
	}

	val, exists := s.Data[key]
	if !exists {
		return "", fmt.Errorf("%w: %s", ErrKeyNotFound, key)
	}

	return val.ValueType, nil
}