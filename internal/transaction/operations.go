package transaction

import (
	"fmt"

	"github.com/neebdev/valoradb/internal/engine"
	"github.com/neebdev/valoradb/internal/parser"
)

// TxSet implements a transactional SET operation
func (tm *TransactionManager) TxSet(threadID uint64, key string, value engine.Value) error {
	// Get the active transaction for this thread
	tx, err := tm.GetActiveTransaction(threadID)
	if err != nil {
		return err
	}

	// Acquire lock on this key
	if err := tm.acquireLock(tx, key); err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	// Check if we already have a change record for this key
	var oldValue *engine.Value
	if existingChange, exists := tx.Changes[key]; exists {
		// Use the original old value from the first change
		oldValue = existingChange.OldValue
	} else {
		// This is the first change to this key in this transaction
		// Get current value, if it exists
		currentVal, err := tm.store.Get(key)
		if err != nil && err != engine.ErrKeyNotFound {
			return err
		}
		
		if err == nil {
			// Make a copy of the current value
			valueCopy := *currentVal
			oldValue = &valueCopy
		}
	}

	// Record this change
	tx.Changes[key] = &ChangeLog{
		Key:       key,
		OldValue:  oldValue,
		NewValue:  &value,
		Operation: "SET",
	}

	// Apply the change to the store
	return tm.store.Set(key, value)
}

// TxGet implements a transactional GET operation
func (tm *TransactionManager) TxGet(threadID uint64, key string) (*engine.Value, error) {
	// Get the active transaction for this thread
	tx, err := tm.GetActiveTransaction(threadID)
	if err != nil {
		// No active transaction, just read directly
		return tm.store.Get(key)
	}

	// Check if this key has been modified in the transaction
	if change, exists := tx.Changes[key]; exists {
		if change.NewValue == nil {
			// Key was deleted in this transaction
			return nil, engine.ErrKeyNotFound
		}
		// Return the transaction's version of the value
		valueCopy := *change.NewValue
		return &valueCopy, nil
	}

	// No changes in transaction, read from store
	return tm.store.Get(key)
}

// TxDel implements a transactional DEL operation
func (tm *TransactionManager) TxDel(threadID uint64, key string) error {
	// Get the active transaction for this thread
	tx, err := tm.GetActiveTransaction(threadID)
	if err != nil {
		return err
	}

	// Acquire lock on this key
	if err := tm.acquireLock(tx, key); err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	// Check if we already have a change record for this key
	var oldValue *engine.Value
	if existingChange, exists := tx.Changes[key]; exists {
		// Use the original old value from the first change
		oldValue = existingChange.OldValue
	} else {
		// This is the first change to this key in this transaction
		// Get current value, if it exists
		currentVal, err := tm.store.Get(key)
		if err != nil {
			return err // Key not found or other error
		}
		
		// Make a copy of the current value
		valueCopy := *currentVal
		oldValue = &valueCopy
	}

	// Record this change
	tx.Changes[key] = &ChangeLog{
		Key:       key,
		OldValue:  oldValue,
		NewValue:  nil, // nil means key was deleted
		Operation: "DEL",
	}

	// Apply the change to the store
	return tm.store.Del(key)
}

// TxExists implements a transactional EXISTS operation
func (tm *TransactionManager) TxExists(threadID uint64, key string) (bool, error) {
	// Get the active transaction for this thread
	tx, err := tm.GetActiveTransaction(threadID)
	if err != nil {
		// No active transaction, just check directly
		return tm.store.Exists(key)
	}

	// Check if this key has been modified in the transaction
	if change, exists := tx.Changes[key]; exists {
		// If NewValue is nil, the key was deleted in this transaction
		return change.NewValue != nil, nil
	}

	// No changes in transaction, check the store
	return tm.store.Exists(key)
}

// TxKeys implements a transactional KEYS operation
func (tm *TransactionManager) TxKeys(threadID uint64, pattern string) ([]string, error) {
	// Get the active transaction for this thread
	tx, err := tm.GetActiveTransaction(threadID)
	if err != nil {
		// No active transaction, just get keys directly
		return tm.store.Keys(pattern)
	}

	// First get keys from storage
	keys, err := tm.store.Keys(pattern)
	if err != nil {
		return nil, err
	}

	// Convert to map for easier manipulation
	keyMap := make(map[string]struct{})
	for _, key := range keys {
		keyMap[key] = struct{}{}
	}

	// Apply transaction changes
	for key, change := range tx.Changes {
		if change.NewValue == nil {
			// Key was deleted in this transaction
			delete(keyMap, key)
		} else if change.OldValue == nil {
			// Key was created in this transaction
			keyMap[key] = struct{}{}
		}
		// Key was updated - no change to keyMap
	}

	// Convert back to slice
	result := make([]string, 0, len(keyMap))
	for key := range keyMap {
		result = append(result, key)
	}

	return result, nil
}

// TxType implements a transactional TYPE operation
func (tm *TransactionManager) TxType(threadID uint64, key string) (parser.ValueType, error) {
	// Get the active transaction for this thread
	tx, err := tm.GetActiveTransaction(threadID)
	if err != nil {
		// No active transaction, just get type directly
		return tm.store.Type(key)
	}

	// Check if this key has been modified in the transaction
	if change, exists := tx.Changes[key]; exists {
		if change.NewValue == nil {
			// Key was deleted in this transaction
			return "", engine.ErrKeyNotFound
		}
		// Return the transaction's version of the value type
		return change.NewValue.ValueType, nil
	}

	// No changes in transaction, get type from store
	return tm.store.Type(key)
}

// TxAdd implements a transactional ADD operation
func (tm *TransactionManager) TxAdd(threadID uint64, key string, value engine.Value) error {
	// Get the active transaction for this thread
	tx, err := tm.GetActiveTransaction(threadID)
	if err != nil {
		return err
	}

	// Acquire lock on this key
	if err := tm.acquireLock(tx, key); err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	// First get the current value
	currentVal, err := tm.TxGet(threadID, key)
	if err != nil {
		return err
	}

	// Check if we already have a change record for this key
	var oldValue *engine.Value
	if existingChange, exists := tx.Changes[key]; exists {
		// Use the original old value from the first change
		oldValue = existingChange.OldValue
	} else {
		// This is the first change to this key in this transaction
		valueCopy := *currentVal
		oldValue = &valueCopy
	}

	// Perform ADD operation using storage engine
	if err := tm.store.Add(key, value); err != nil {
		return err
	}

	// Get the updated value
	updatedVal, err := tm.store.Get(key)
	if err != nil {
		return err
	}

	// Record this change
	tx.Changes[key] = &ChangeLog{
		Key:       key,
		OldValue:  oldValue,
		NewValue:  updatedVal,
		Operation: "ADD",
	}

	return nil
}

// TxSub implements a transactional SUB operation
func (tm *TransactionManager) TxSub(threadID uint64, key string, value engine.Value) error {
	// Get the active transaction for this thread
	tx, err := tm.GetActiveTransaction(threadID)
	if err != nil {
		return err
	}

	// Acquire lock on this key
	if err := tm.acquireLock(tx, key); err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	// First get the current value
	currentVal, err := tm.TxGet(threadID, key)
	if err != nil {
		return err
	}

	// Check if we already have a change record for this key
	var oldValue *engine.Value
	if existingChange, exists := tx.Changes[key]; exists {
		// Use the original old value from the first change
		oldValue = existingChange.OldValue
	} else {
		// This is the first change to this key in this transaction
		valueCopy := *currentVal
		oldValue = &valueCopy
	}

	// Perform SUB operation using storage engine
	if err := tm.store.Sub(key, value); err != nil {
		return err
	}

	// Get the updated value
	updatedVal, err := tm.store.Get(key)
	if err != nil {
		return err
	}

	// Record this change
	tx.Changes[key] = &ChangeLog{
		Key:       key,
		OldValue:  oldValue,
		NewValue:  updatedVal,
		Operation: "SUB",
	}

	return nil
}

// TxMul implements a transactional MUL operation
func (tm *TransactionManager) TxMul(threadID uint64, key string, value engine.Value) error {
	// Get the active transaction for this thread
	tx, err := tm.GetActiveTransaction(threadID)
	if err != nil {
		return err
	}

	// Acquire lock on this key
	if err := tm.acquireLock(tx, key); err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	// First get the current value
	currentVal, err := tm.TxGet(threadID, key)
	if err != nil {
		return err
	}

	// Check if we already have a change record for this key
	var oldValue *engine.Value
	if existingChange, exists := tx.Changes[key]; exists {
		// Use the original old value from the first change
		oldValue = existingChange.OldValue
	} else {
		// This is the first change to this key in this transaction
		valueCopy := *currentVal
		oldValue = &valueCopy
	}

	// Perform MUL operation using storage engine
	if err := tm.store.Mul(key, value); err != nil {
		return err
	}

	// Get the updated value
	updatedVal, err := tm.store.Get(key)
	if err != nil {
		return err
	}

	// Record this change
	tx.Changes[key] = &ChangeLog{
		Key:       key,
		OldValue:  oldValue,
		NewValue:  updatedVal,
		Operation: "MUL",
	}

	return nil
}

// TxDiv implements a transactional DIV operation
func (tm *TransactionManager) TxDiv(threadID uint64, key string, value engine.Value) error {
	// Get the active transaction for this thread
	tx, err := tm.GetActiveTransaction(threadID)
	if err != nil {
		return err
	}

	// Acquire lock on this key
	if err := tm.acquireLock(tx, key); err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}

	// First get the current value
	currentVal, err := tm.TxGet(threadID, key)
	if err != nil {
		return err
	}

	// Check if we already have a change record for this key
	var oldValue *engine.Value
	if existingChange, exists := tx.Changes[key]; exists {
		// Use the original old value from the first change
		oldValue = existingChange.OldValue
	} else {
		// This is the first change to this key in this transaction
		valueCopy := *currentVal
		oldValue = &valueCopy
	}

	// Perform DIV operation using storage engine
	if err := tm.store.Div(key, value); err != nil {
		return err
	}

	// Get the updated value
	updatedVal, err := tm.store.Get(key)
	if err != nil {
		return err
	}

	// Record this change
	tx.Changes[key] = &ChangeLog{
		Key:       key,
		OldValue:  oldValue,
		NewValue:  updatedVal,
		Operation: "DIV",
	}

	return nil
}

// Helper method to acquire a lock on a key
func (tm *TransactionManager) acquireLock(tx *Transaction, key string) error {
	// Acquire a write lock since all of our operations modify data
	err := tm.lockManager.AcquireLock(tx.ID, key, WriteLock)
	if err != nil {
		return err
	}
	
	// Record that this transaction has locked this key
	tx.KeyLocks[key] = struct{}{}
	return nil
} 