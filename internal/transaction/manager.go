package transaction

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/neebdev/valoradb/internal/engine"
)

var (
	ErrNoActiveTransaction     = errors.New("no active transaction")
	ErrTransactionAlreadyActive = errors.New("transaction already active")
	ErrTransactionNotFound     = errors.New("transaction not found")
)

// TransactionStatus represents the current state of a transaction
type TransactionStatus int

const (
	TxActive TransactionStatus = iota
	TxCommitted
	TxRolledBack
)

// Transaction represents a database transaction
type Transaction struct {
	ID        uint64
	Status    TransactionStatus
	Changes   map[string]*ChangeLog // Key -> ChangeLog
	KeyLocks  map[string]struct{}   // Keys locked by this transaction
}

// ChangeLog tracks changes to a key within a transaction
type ChangeLog struct {
	Key       string
	OldValue  *engine.Value  // Previous value (nil if key didn't exist)
	NewValue  *engine.Value  // New value (nil if key was deleted)
	Operation string         // The operation that caused this change (SET, DEL, etc.)
}

// TransactionManager manages database transactions
type TransactionManager struct {
	store          *engine.Store
	txMu           sync.RWMutex
	activeTx       map[uint64]*Transaction
	nextTxID       uint64
	globalLock     sync.Mutex // Lock for beginning/committing transactions
	activeThreadTx map[uint64]uint64 // Thread ID -> TX ID mapping
	lockManager    *LockManager
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(store *engine.Store) *TransactionManager {
	return &TransactionManager{
		store:          store,
		activeTx:       make(map[uint64]*Transaction),
		nextTxID:       1,
		activeThreadTx: make(map[uint64]uint64),
		lockManager:    NewLockManager(),
	}
}

// Begin starts a new transaction
func (tm *TransactionManager) Begin(threadID uint64) (uint64, error) {
	tm.globalLock.Lock()
	defer tm.globalLock.Unlock()

	// Check if this thread already has an active transaction
	if txID, exists := tm.activeThreadTx[threadID]; exists {
		return 0, fmt.Errorf("%w: thread %d already has transaction %d", 
			ErrTransactionAlreadyActive, threadID, txID)
	}

	// Create a new transaction
	txID := atomic.AddUint64(&tm.nextTxID, 1)
	tx := &Transaction{
		ID:       txID,
		Status:   TxActive,
		Changes:  make(map[string]*ChangeLog),
		KeyLocks: make(map[string]struct{}),
	}

	tm.txMu.Lock()
	defer tm.txMu.Unlock()
	
	tm.activeTx[txID] = tx
	tm.activeThreadTx[threadID] = txID

	return txID, nil
}

// Commit finalizes the transaction
func (tm *TransactionManager) Commit(threadID uint64) error {
	tm.globalLock.Lock()
	defer tm.globalLock.Unlock()

	// Get transaction ID for this thread
	txID, exists := tm.activeThreadTx[threadID]
	if !exists {
		return ErrNoActiveTransaction
	}

	tm.txMu.Lock()
	defer tm.txMu.Unlock()

	tx, exists := tm.activeTx[txID]
	if !exists {
		return ErrTransactionNotFound
	}

	// Mark transaction as committed
	tx.Status = TxCommitted

	// The transaction is already applied to the store as we go,
	// so we just need to clean up resources

	// Release all locks held by this transaction
	tm.lockManager.ReleaseAllLocks(txID)

	// Remove transaction from active lists
	delete(tm.activeTx, txID)
	delete(tm.activeThreadTx, threadID)

	return nil
}

// Rollback aborts the transaction
func (tm *TransactionManager) Rollback(threadID uint64) error {
	tm.globalLock.Lock()
	defer tm.globalLock.Unlock()

	// Get transaction ID for this thread
	txID, exists := tm.activeThreadTx[threadID]
	if !exists {
		return ErrNoActiveTransaction
	}

	tm.txMu.Lock()
	defer tm.txMu.Unlock()

	tx, exists := tm.activeTx[txID]
	if !exists {
		return ErrTransactionNotFound
	}

	// Undo all changes in reverse order
	for key, change := range tx.Changes {
		if change.OldValue == nil {
			// Key was created in this transaction, delete it
			if err := tm.store.Del(key); err != nil {
				// Log error but continue rollback
				fmt.Printf("Error rolling back key %s: %v\n", key, err)
			}
		} else {
			// Restore old value
			if err := tm.store.Set(key, *change.OldValue); err != nil {
				// Log error but continue rollback
				fmt.Printf("Error restoring key %s: %v\n", key, err)
			}
		}
	}

	// Mark transaction as rolled back
	tx.Status = TxRolledBack

	// Release all locks held by this transaction
	tm.lockManager.ReleaseAllLocks(txID)

	// Remove transaction from active lists
	delete(tm.activeTx, txID)
	delete(tm.activeThreadTx, threadID)

	return nil
}

// GetActiveTransaction returns the active transaction for a thread
func (tm *TransactionManager) GetActiveTransaction(threadID uint64) (*Transaction, error) {
	tm.txMu.RLock()
	defer tm.txMu.RUnlock()

	txID, exists := tm.activeThreadTx[threadID]
	if !exists {
		return nil, ErrNoActiveTransaction
	}

	tx, exists := tm.activeTx[txID]
	if !exists {
		return nil, ErrTransactionNotFound
	}

	return tx, nil
} 