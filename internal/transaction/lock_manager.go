package transaction

import (
	"errors"
	"fmt"
	"sync"
)

// Lock types
const (
	ReadLock  = "read"
	WriteLock = "write"
)

var (
	ErrLockConflict = errors.New("lock conflict")
	ErrLockNotHeld  = errors.New("lock not held by transaction")
)

// LockManager handles two-phase locking for the transaction manager
type LockManager struct {
	mu         sync.Mutex
	keyLocks   map[string]map[uint64]string // key -> {txID -> lockType}
	txLocks    map[uint64]map[string]string // txID -> {key -> lockType}
	lockWaiters map[string][]*lockWaiter    // key -> waiters
}

type lockWaiter struct {
	txID     uint64
	lockType string
	ch       chan struct{}
}

// NewLockManager creates a new lock manager
func NewLockManager() *LockManager {
	return &LockManager{
		keyLocks:   make(map[string]map[uint64]string),
		txLocks:    make(map[uint64]map[string]string),
		lockWaiters: make(map[string][]*lockWaiter),
	}
}

// AcquireLock attempts to acquire a lock of the specified type on the key for the transaction
func (lm *LockManager) AcquireLock(txID uint64, key string, lockType string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Initialize maps if needed
	if lm.keyLocks[key] == nil {
		lm.keyLocks[key] = make(map[uint64]string)
	}
	if lm.txLocks[txID] == nil {
		lm.txLocks[txID] = make(map[string]string)
	}

	// Check if this transaction already holds a lock on this key
	if existingLockType, exists := lm.txLocks[txID][key]; exists {
		// If already have write lock or requesting read lock but already have it
		if existingLockType == WriteLock || (lockType == ReadLock && existingLockType == ReadLock) {
			return nil // Already have adequate lock
		}
		
		// Upgrade from read to write lock
		if canUpgradeLock(lm.keyLocks[key], txID) {
			lm.keyLocks[key][txID] = WriteLock
			lm.txLocks[txID][key] = WriteLock
			return nil
		}
		return ErrLockConflict
	}

	// Check if the lock can be granted
	if canGrantLock(lm.keyLocks[key], txID, lockType) {
		lm.keyLocks[key][txID] = lockType
		lm.txLocks[txID][key] = lockType
		return nil
	}

	// Lock cannot be granted immediately
	// In a full implementation, we would add the transaction to a wait queue
	// and implement deadlock detection, but for simplicity we'll just return an error
	return ErrLockConflict
}

// ReleaseLock releases a lock held by a transaction on a key
func (lm *LockManager) ReleaseLock(txID uint64, key string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Check if the transaction holds a lock on this key
	if _, exists := lm.txLocks[txID][key]; !exists {
		return ErrLockNotHeld
	}

	// Remove the lock
	delete(lm.keyLocks[key], txID)
	delete(lm.txLocks[txID], key)

	// Clean up empty maps
	if len(lm.keyLocks[key]) == 0 {
		delete(lm.keyLocks, key)
	}
	if len(lm.txLocks[txID]) == 0 {
		delete(lm.txLocks, txID)
	}

	// In a full implementation, we would check the wait queue and grant locks to waiting transactions
	return nil
}

// ReleaseAllLocks releases all locks held by a transaction
func (lm *LockManager) ReleaseAllLocks(txID uint64) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// Get all keys locked by this transaction
	keys := make([]string, 0, len(lm.txLocks[txID]))
	for key := range lm.txLocks[txID] {
		keys = append(keys, key)
	}

	// Release each lock
	for _, key := range keys {
		delete(lm.keyLocks[key], txID)
		if len(lm.keyLocks[key]) == 0 {
			delete(lm.keyLocks, key)
		}
	}

	// Remove transaction from tracking
	delete(lm.txLocks, txID)
}

// Helper function to check if a lock can be granted
func canGrantLock(keyLocks map[uint64]string, txID uint64, lockType string) bool {
	if len(keyLocks) == 0 {
		return true // No locks on this key
	}

	if lockType == ReadLock {
		// Read locks can be granted if no transaction holds a write lock
		for _, lt := range keyLocks {
			if lt == WriteLock {
				return false
			}
		}
		return true
	}

	// Write lock can only be granted if no other transaction holds any lock
	return len(keyLocks) == 0
}

// Helper function to check if a read lock can be upgraded to a write lock
func canUpgradeLock(keyLocks map[uint64]string, txID uint64) bool {
	// Can upgrade if this is the only transaction with a lock
	return len(keyLocks) == 1 && keyLocks[txID] == ReadLock
}

// IsLockHeld checks if a transaction holds a lock on a key
func (lm *LockManager) IsLockHeld(txID uint64, key string) bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	txLocks, exists := lm.txLocks[txID]
	if !exists {
		return false
	}

	_, exists = txLocks[key]
	return exists
}

// GetLockType returns the type of lock held by a transaction on a key
func (lm *LockManager) GetLockType(txID uint64, key string) (string, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	txLocks, exists := lm.txLocks[txID]
	if !exists {
		return "", fmt.Errorf("transaction %d has no locks", txID)
	}

	lockType, exists := txLocks[key]
	if !exists {
		return "", fmt.Errorf("transaction %d has no lock on key %s", txID, key)
	}

	return lockType, nil
} 