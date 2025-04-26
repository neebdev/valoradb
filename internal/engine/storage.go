package engine

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/neebdev/valoradb/internal/parser"
)

type Value struct {
	Data      string
	ValueType parser.ValueType
}

type Store struct {
	Data map[string]Value
	Wal  *os.File
	Mu   sync.RWMutex
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
	if !exists {
		err := errors.New("key not found")
		return nil, err
	}
	return &val, nil
}

func (s *Store) Del(key string) error {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	_, exists := s.Data[key]
	if !exists {
		return fmt.Errorf("key '%s' not found", key)

	}

	logLine := fmt.Sprintf("DEL %s\n", key)
	if _, err := s.Wal.WriteString(logLine); err != nil {
		return err
	}
	if err := s.Wal.Sync(); err != nil {
		return err
	}

	delete(s.Data, key)
	return nil
}

func (s *Store) Exists(key string) (bool, error) {
	s.Mu.RLock()
	defer s.Mu.RUnlock()

	logLine := fmt.Sprintf("EXISTS %s\n", key)
	if _, err := s.Wal.WriteString(logLine); err != nil {
		return false, err
	}
	if err := s.Wal.Sync(); err != nil {
		return false, err
	}
	_, exists := s.Data[key]
	return exists, nil
}
func (s *Store) Type(key string) (parser.ValueType, error) {
	s.Mu.RLock()
	defer s.Mu.RUnlock()

	logLine := fmt.Sprintf("TYPE %s\n", key)
	if _, err := s.Wal.WriteString(logLine); err != nil {
		return "", err
	}
	if err := s.Wal.Sync(); err != nil {
		return "", err
	}

	val, exists := s.Data[key]
	if !exists {
		return "", fmt.Errorf("key '%s' not found", key)
	}

	return val.ValueType, nil
}
