package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/neebdev/valoradb/internal/engine"
	"github.com/neebdev/valoradb/internal/parser"
	"github.com/neebdev/valoradb/internal/transaction"
)

type Client struct {
	store      *engine.Store
	txManager  *transaction.TransactionManager
	walPath    string
	inTransaction bool
	threadID   uint64
}

// NewClient creates a new client instance
func NewClient(walPath string) (*Client, error) {
	store, err := engine.NewStore(walPath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize store: %v", err)
	}

	// Recover state from WAL
	fmt.Println("Recovering state from WAL...")
	if err := store.RecoverFromWAL(); err != nil {
		fmt.Printf("Warning: Failed to recover from WAL: %v\n", err)
		fmt.Println("Starting with empty state.")
	}

	// Initialize transaction manager
	txManager := transaction.NewTransactionManager(store)

	return &Client{
		store:      store,
		txManager:  txManager,
		walPath:    walPath,
		inTransaction: false,
		threadID:   1, // Using a simple thread ID for demo purposes
	}, nil
}

// Start begins the interactive shell
func (c *Client) Start() {
	fmt.Println("Welcome to ValoraDĂ Interactive Shell")
	fmt.Println("Type 'EXIT' or 'exit' to leave the shell")
	fmt.Println("--------------------------------")

	scanner := bufio.NewScanner(os.Stdin)
	
	for {
		fmt.Print("valoradb> ")
		if !scanner.Scan() {
			break
		}
		
		input := scanner.Text()
		input = strings.TrimSpace(input)
		
		if input == "" {
			continue
		}
		
		if input == "EXIT" || input == "exit" {
			break
		}
		
		c.executeCommand(input)
	}
	
	// Clean up before exiting
	c.store.Wal.Close()
	fmt.Println("Goodbye!")
}

// executeCommand parses and executes a single command
func (c *Client) executeCommand(rawQuery string) {
	// Remove trailing semicolon if present
	rawQuery = strings.TrimSuffix(strings.TrimSpace(rawQuery), ";")
	
	cmd, err := parser.ParseCommand(rawQuery)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}

	if cmd.Type == parser.CmdSet || cmd.Type == parser.CmdAdd || cmd.Type == parser.CmdSub || cmd.Type == parser.CmdMul || cmd.Type == parser.CmdDiv {
		fmt.Printf("Debug - Key: %s, Value: %s, ValueType: %s\n", cmd.Key, cmd.Value, cmd.ValueType)
	}
	
	switch cmd.Type {
	case parser.CmdBegin:
		if c.inTransaction {
			fmt.Println("Error: already in a transaction")
			return
		}
		
		_, err := c.txManager.Begin(c.threadID)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		
		c.inTransaction = true
		fmt.Println("Transaction started")
		
	case parser.CmdCommit:
		if !c.inTransaction {
			fmt.Println("Error: no active transaction")
			return
		}
		
		if err := c.txManager.Commit(c.threadID); err != nil {
			fmt.Println("Error:", err)
			return
		}
		
		c.inTransaction = false
		fmt.Println("Transaction committed")
		
	case parser.CmdRollback:
		if !c.inTransaction {
			fmt.Println("Error: no active transaction")
			return
		}
		
		if err := c.txManager.Rollback(c.threadID); err != nil {
			fmt.Println("Error:", err)
			return
		}
		
		c.inTransaction = false
		fmt.Println("Transaction rolled back")
		
	case parser.CmdClear:
		if c.inTransaction {
			fmt.Println("Error: cannot clear database during a transaction")
			return
		}
		
		if err := c.store.Clear(); err != nil {
			fmt.Println("Error:", err)
			return
		}
		
	case parser.CmdSet:
		value := engine.Value{
			Data:      cmd.Value,
			ValueType: cmd.ValueType,
		}
		
		var err error
		if c.inTransaction {
			err = c.txManager.TxSet(c.threadID, cmd.Key, value)
		} else {
			err = c.store.Set(cmd.Key, value)
		}
		
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Printf("Set %s to %s\n", cmd.Key, cmd.Value)
		
	case parser.CmdGet:
		var val *engine.Value
		var err error
		
		if c.inTransaction {
			val, err = c.txManager.TxGet(c.threadID, cmd.Key)
		} else {
			val, err = c.store.Get(cmd.Key)
		}
		
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Printf("%s = %s\n", cmd.Key, val.Data)
		
	case parser.CmdKeys:
		var keys []string
		var err error
		
		if c.inTransaction {
			keys, err = c.txManager.TxKeys(c.threadID, cmd.Value)
		} else {
			keys, err = c.store.Keys(cmd.Value)
		}
		
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Printf("Found %d keys matching pattern %s\n", len(keys), cmd.Value)
		for _, key := range keys {
			fmt.Printf("  - %s\n", key)
		}
		
	case parser.CmdDel:
		var err error
		
		if c.inTransaction {
			err = c.txManager.TxDel(c.threadID, cmd.Key)
		} else {
			err = c.store.Del(cmd.Key)
		}
		
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Printf("Key %s deleted successfully\n", cmd.Key)

	case parser.CmdExists:
		var exists bool
		var err error
		
		if c.inTransaction {
			exists, err = c.txManager.TxExists(c.threadID, cmd.Key)
		} else {
			exists, err = c.store.Exists(cmd.Key)
		}
		
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		
		if exists {
			fmt.Printf("Key %s exists\n", cmd.Key)
		} else {
			fmt.Printf("Key %s does not exist\n", cmd.Key)
		}
		
	case parser.CmdType:
		var valueType parser.ValueType
		var err error
		
		if c.inTransaction {
			valueType, err = c.txManager.TxType(c.threadID, cmd.Key)
		} else {
			valueType, err = c.store.Type(cmd.Key)
		}
		
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Printf("Key %s has type %s\n", cmd.Key, valueType)

	case parser.CmdAdd:
		value := engine.Value{
			Data:      cmd.Value,
			ValueType: cmd.ValueType,
		}
		
		var err error
		if c.inTransaction {
			err = c.txManager.TxAdd(c.threadID, cmd.Key, value)
		} else {
			err = c.store.Add(cmd.Key, value)
		}
		
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Printf("Added %s to %s\n", cmd.Value, cmd.Key)

	case parser.CmdSub:
		value := engine.Value{
			Data:      cmd.Value,
			ValueType: cmd.ValueType,
		}
		
		var err error
		if c.inTransaction {
			err = c.txManager.TxSub(c.threadID, cmd.Key, value)
		} else {
			err = c.store.Sub(cmd.Key, value)
		}
		
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Printf("Subtracted %s from %s\n", cmd.Value, cmd.Key)

	case parser.CmdMul:
		value := engine.Value{
			Data:      cmd.Value,
			ValueType: cmd.ValueType,
		}
		
		var err error
		if c.inTransaction {
			err = c.txManager.TxMul(c.threadID, cmd.Key, value)
		} else {
			err = c.store.Mul(cmd.Key, value)
		}
		
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Printf("Multiplied %s by %s\n", cmd.Key, cmd.Value)
		
	case parser.CmdDiv:
		value := engine.Value{
			Data:      cmd.Value,
			ValueType: cmd.ValueType,
		}
		
		// Check for division by zero before calling Div
		if cmd.Value == "0" {
			fmt.Printf("Error: cannot divide %s by zero\n", cmd.Key)
			return
		}
		
		var err error
		if c.inTransaction {
			err = c.txManager.TxDiv(c.threadID, cmd.Key, value)
		} else {
			err = c.store.Div(cmd.Key, value)
		}
		
		if err != nil {
			fmt.Println("Error:", err)
			return
		}
		fmt.Printf("Divided %s by %s\n", cmd.Key, cmd.Value)
		
	default:
		fmt.Println("Unknown command type:", cmd.Type)
	}
} 