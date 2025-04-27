package wal

// RecordType identifies the type of WAL record
type RecordType byte

const (
	// Record types
	RecordNoop        RecordType = 0x00
	RecordTransBegin  RecordType = 0x01
	RecordTransCommit RecordType = 0x02
	RecordTransAbort  RecordType = 0x03
	RecordSet         RecordType = 0x10
	RecordDel         RecordType = 0x11
	RecordAdd         RecordType = 0x12
	RecordSub         RecordType = 0x13
	RecordMul         RecordType = 0x14
	RecordDiv         RecordType = 0x15
	RecordClear       RecordType = 0x20 // New record type for clearing all data
)

// ValueType represents the data type of a value
type ValueType byte

const (
	TypeString ValueType = 0x01
	TypeNumber ValueType = 0x02
	TypeBool   ValueType = 0x03
)

// DataOperationRecord represents a data modification operation
type DataOperationRecord struct {
	Key       string
	ValueType ValueType
	Value     []byte
}

// CheckpointRecord represents a database checkpoint
type CheckpointRecord struct {
	CheckpointLSN LogSequenceNumber
	PrevCheckLSN  LogSequenceNumber
	Timestamp     int64
	KeyCount      uint32
} 