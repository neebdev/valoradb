package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/neebdev/valoradb/internal/parser"
)

type Value struct {
	Data      string
	ValueType parser.ValueType
}

type Store struct {
	Data    map[string]Value
	Wal     *os.File
	Mu      sync.RWMutex
}


func (s *Store) Set(key string, value Value) error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	logLine := fmt.Sprintf("SET %s %s TYPE %s\n", key, value.Data, value.ValueType)
	if _, err := s.Wal.WriteString(logLine); err != nil {
		return err
	}
	if err := s.Wal.Sync(); err != nil {
		return err
	}

	s.Data[key] = value
	return nil
}

func (s *Store) Get(key string) (*Value, error) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	logLine := fmt.Sprintf("GET %s\n", key)
	if _, err := s.Wal.WriteString(logLine); err != nil {
		return nil, err
	}
	if err := s.Wal.Sync(); err != nil {
		return nil, err
	}

	val, exists := s.Data[key]
	if !exists{
		err := errors.New("key not found")
		return nil, err
	}
	return &val, nil
}

func (s *Store) Keys(pattern string) ([]string, error) {
	s.Mu.RLock()
	defer s.Mu.RUnlock()

	logLine := fmt.Sprintf("KEYS %s\n", pattern)
	if _, err := s.Wal.WriteString(logLine); err != nil {
		return nil, fmt.Errorf("failed to write to WAL: %v", err)
	}
	if err := s.Wal.Sync(); err != nil {
		return nil, fmt.Errorf("failed to sync WAL: %v", err)
	}

	var matches []string
	for key := range s.Data {
		if ok, _ := filepath.Match(pattern, key); ok {
			matches = append(matches, key)
		}
	}

	return matches, nil
}