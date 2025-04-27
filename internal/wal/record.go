package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// Record types
const (
	RecordBeginTransaction uint8 = iota + 1
	RecordCommitTransaction
	RecordRollbackTransaction
	RecordInsert
	RecordUpdate
	RecordDelete
	RecordCheckpoint
)

// HeaderSize is the size of the record header in bytes (magic + lsn + txid + type + length + checksum)
const HeaderSize = 4 + 8 + 8 + 1 + 4 + 4

// LogSequenceNumber uniquely identifies a record in the WAL
type LogSequenceNumber uint64

// RecordHeader represents the header of a WAL record
type RecordHeader struct {
	// Magic number for format validation (4 bytes)
	Magic uint32
	
	// Log Sequence Number (8 bytes)
	LSN LogSequenceNumber
	
	// Transaction ID (8 bytes)
	TxID uint64
	
	// Record type (1 byte)
	Type uint8
	
	// Length of payload (4 bytes)
	Length uint32
	
	// Checksum of payload (4 bytes)
	Checksum uint32
	
	// Timestamp in nanoseconds since epoch (8 bytes) - not included in disk format
	Timestamp int64
}

// Record represents a complete WAL record
type Record struct {
	Header RecordHeader
	Payload []byte
}

// MagicNumber is a constant that identifies WAL records
const MagicNumber uint32 = 0x57414C00 // "WAL\0"

// NewRecord creates a new record with the given type, transaction ID, and payload
func NewRecord(recordType uint8, txID uint64, payload []byte) Record {
	return Record{
		Header: RecordHeader{
			Magic:     MagicNumber,
			LSN:       0, // Will be set by Manager.AppendRecord
			TxID:      txID,
			Type:      recordType,
			Length:    uint32(len(payload)),
			Checksum:  0, // Will be calculated by Manager.AppendRecord
			Timestamp: time.Now().UnixNano(),
		},
		Payload: payload,
	}
}

// NewBeginTransactionRecord creates a new begin transaction record
func NewBeginTransactionRecord(txID uint64) Record {
	// No additional payload for begin transaction
	return NewRecord(RecordBeginTransaction, txID, []byte{})
}

// NewCommitTransactionRecord creates a new commit transaction record
func NewCommitTransactionRecord(txID uint64) Record {
	// No additional payload for commit transaction
	return NewRecord(RecordCommitTransaction, txID, []byte{})
}

// NewRollbackTransactionRecord creates a new rollback transaction record
func NewRollbackTransactionRecord(txID uint64) Record {
	// No additional payload for rollback transaction
	return NewRecord(RecordRollbackTransaction, txID, []byte{})
}

// NewInsertRecord creates a new insert record
func NewInsertRecord(txID uint64, key, value []byte) Record {
	// Payload format: key_length (4 bytes) + key + value
	payload := make([]byte, 4+len(key)+len(value))
	binary.BigEndian.PutUint32(payload[:4], uint32(len(key)))
	copy(payload[4:], key)
	copy(payload[4+len(key):], value)
	
	return NewRecord(RecordInsert, txID, payload)
}

// NewUpdateRecord creates a new update record
func NewUpdateRecord(txID uint64, key, oldValue, newValue []byte) Record {
	// Payload format: key_length (4 bytes) + old_value_length (4 bytes) + key + old_value + new_value
	payload := make([]byte, 8+len(key)+len(oldValue)+len(newValue))
	binary.BigEndian.PutUint32(payload[:4], uint32(len(key)))
	binary.BigEndian.PutUint32(payload[4:8], uint32(len(oldValue)))
	copy(payload[8:], key)
	copy(payload[8+len(key):], oldValue)
	copy(payload[8+len(key)+len(oldValue):], newValue)
	
	return NewRecord(RecordUpdate, txID, payload)
}

// NewDeleteRecord creates a new delete record
func NewDeleteRecord(txID uint64, key, value []byte) Record {
	// Payload format: key_length (4 bytes) + key + value_to_delete
	payload := make([]byte, 4+len(key)+len(value))
	binary.BigEndian.PutUint32(payload[:4], uint32(len(key)))
	copy(payload[4:], key)
	copy(payload[4+len(key):], value)
	
	return NewRecord(RecordDelete, txID, payload)
}

// NewCheckpointRecord creates a new checkpoint record
func NewCheckpointRecord(lsn, prevCheckpointLSN LogSequenceNumber, keyCount uint64) Record {
	// Payload format: checkpoint_lsn (8 bytes) + prev_checkpoint_lsn (8 bytes) + key_count (8 bytes)
	payload := make([]byte, 24)
	binary.BigEndian.PutUint64(payload[:8], uint64(lsn))
	binary.BigEndian.PutUint64(payload[8:16], uint64(prevCheckpointLSN))
	binary.BigEndian.PutUint64(payload[16:24], keyCount)
	
	return NewRecord(RecordCheckpoint, 0, payload)
}

// WriteHeader writes a record header to the given writer
func WriteHeader(w io.Writer, header RecordHeader) error {
	// Format: magic(4) + lsn(8) + txid(8) + type(1) + length(4) + checksum(4)
	buf := make([]byte, HeaderSize)
	
	binary.BigEndian.PutUint32(buf[0:4], MagicNumber)
	binary.BigEndian.PutUint64(buf[4:12], uint64(header.LSN))
	binary.BigEndian.PutUint64(buf[12:20], header.TxID)
	buf[20] = header.Type
	binary.BigEndian.PutUint32(buf[21:25], header.Length)
	binary.BigEndian.PutUint32(buf[25:29], header.Checksum)
	
	_, err := w.Write(buf)
	return err
}

// ReadHeader reads a record header from the given reader
func ReadHeader(r io.Reader) (RecordHeader, error) {
	buf := make([]byte, HeaderSize)
	var header RecordHeader
	
	if _, err := io.ReadFull(r, buf); err != nil {
		return header, err
	}
	
	magic := binary.BigEndian.Uint32(buf[0:4])
	if magic != MagicNumber {
		return header, fmt.Errorf("invalid record magic number: %x", magic)
	}
	
	header.Magic = magic
	header.LSN = LogSequenceNumber(binary.BigEndian.Uint64(buf[4:12]))
	header.TxID = binary.BigEndian.Uint64(buf[12:20])
	header.Type = buf[20]
	header.Length = binary.BigEndian.Uint32(buf[21:25])
	header.Checksum = binary.BigEndian.Uint32(buf[25:29])
	header.Timestamp = time.Now().UnixNano()
	
	return header, nil
}

// ExtractKey extracts the key from an insert, update, or delete record
func ExtractKey(record Record) ([]byte, error) {
	if record.Header.Type != RecordInsert && record.Header.Type != RecordUpdate && record.Header.Type != RecordDelete {
		return nil, fmt.Errorf("cannot extract key from record type: %d", record.Header.Type)
	}
	
	if len(record.Payload) < 4 {
		return nil, fmt.Errorf("invalid record payload: too short")
	}
	
	keyLength := binary.BigEndian.Uint32(record.Payload[:4])
	if len(record.Payload) < int(4+keyLength) {
		return nil, fmt.Errorf("invalid record payload: key length exceeds payload size")
	}
	
	key := make([]byte, keyLength)
	copy(key, record.Payload[4:4+keyLength])
	
	return key, nil
}

// ExtractValue extracts the value from an insert record
func ExtractValue(record Record) ([]byte, error) {
	if record.Header.Type != RecordInsert {
		return nil, fmt.Errorf("cannot extract value from record type: %d", record.Header.Type)
	}
	
	if len(record.Payload) < 4 {
		return nil, fmt.Errorf("invalid record payload: too short")
	}
	
	keyLength := binary.BigEndian.Uint32(record.Payload[:4])
	if len(record.Payload) < int(4+keyLength) {
		return nil, fmt.Errorf("invalid record payload: key length exceeds payload size")
	}
	
	value := make([]byte, len(record.Payload)-4-int(keyLength))
	copy(value, record.Payload[4+keyLength:])
	
	return value, nil
}

// ExtractOldAndNewValues extracts the old and new values from an update record
func ExtractOldAndNewValues(record Record) ([]byte, []byte, error) {
	if record.Header.Type != RecordUpdate {
		return nil, nil, fmt.Errorf("cannot extract old and new values from record type: %d", record.Header.Type)
	}
	
	if len(record.Payload) < 8 {
		return nil, nil, fmt.Errorf("invalid record payload: too short")
	}
	
	keyLength := binary.BigEndian.Uint32(record.Payload[:4])
	oldValueLength := binary.BigEndian.Uint32(record.Payload[4:8])
	
	if len(record.Payload) < int(8+keyLength+oldValueLength) {
		return nil, nil, fmt.Errorf("invalid record payload: lengths exceed payload size")
	}
	
	oldValue := make([]byte, oldValueLength)
	copy(oldValue, record.Payload[8+keyLength:8+keyLength+oldValueLength])
	
	newValue := make([]byte, len(record.Payload)-8-int(keyLength)-int(oldValueLength))
	copy(newValue, record.Payload[8+keyLength+oldValueLength:])
	
	return oldValue, newValue, nil
}

// ExtractCheckpointInfo extracts checkpoint information from a checkpoint record
func ExtractCheckpointInfo(record Record) (LogSequenceNumber, LogSequenceNumber, uint64, error) {
	if record.Header.Type != RecordCheckpoint {
		return 0, 0, 0, fmt.Errorf("not a checkpoint record: type %d", record.Header.Type)
	}
	
	if len(record.Payload) < 24 {
		return 0, 0, 0, fmt.Errorf("invalid checkpoint record payload: too short")
	}
	
	lsn := LogSequenceNumber(binary.BigEndian.Uint64(record.Payload[:8]))
	prevLSN := LogSequenceNumber(binary.BigEndian.Uint64(record.Payload[8:16]))
	keyCount := binary.BigEndian.Uint64(record.Payload[16:24])
	
	return lsn, prevLSN, keyCount, nil
} 