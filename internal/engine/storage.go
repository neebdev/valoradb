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