package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/neebdev/valoradb/internal/engine"
	"github.com/neebdev/valoradb/internal/parser"
)

func main() {
	// Setup signal handling for clean shutdown
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	
	queries, err := parser.ParseQueriesFromFile("queries.txt")
	if err != nil {
		fmt.Println("Error reading queries:", err)
		return
	}

	walPath := "./wal.log"
	store, err := engine.NewStore(walPath)
	if err != nil {
		fmt.Printf("Failed to initialize store: %v\n", err)
		return
	}
	defer store.Wal.Close()

	// Start a goroutine to handle graceful shutdown
	go func() {
		<-sigs
		fmt.Println("\nShutting down ValoraDĂ...")
		store.Wal.Close()
		os.Exit(0)
	}()

	for _, rawQuery := range queries {
		fmt.Println(">", rawQuery)

		cmd, err := parser.ParseCommand(rawQuery)
		if err != nil {
			fmt.Println("  ❌ Error:", err)
			continue
		}
		fmt.Printf(" Executing: %+v\n", cmd)
    
		switch cmd.Type {
		case parser.CmdSet:
			value := engine.Value{
				Data:      cmd.Value,
				ValueType: cmd.ValueType,
			}
			if err := store.Set(cmd.Key, value); err != nil {
				fmt.Println("  ❌ Error:", err)
				continue
			}
			fmt.Printf("  ✅ Set %s to %s\n", cmd.Key, cmd.Value)
			
		case parser.CmdGet:
			val, err := store.Get(cmd.Key)
			if err != nil {
				fmt.Println("  ❌ Error:", err)
				continue
			}
			fmt.Printf("  ✅ %s = %s\n", cmd.Key, val.Data)
			
		case parser.CmdKeys:
			keys, err := store.Keys(cmd.Value)
			if err != nil {
				fmt.Println("  ❌ Error:", err)
				continue
			}
			fmt.Printf("  ✅ Found %d keys matching pattern %s\n", len(keys), cmd.Value)
			for _, key := range keys {
				fmt.Printf("     - %s\n", key)
			}
			
		case parser.CmdDel:
			err := store.Del(cmd.Key)
			if err != nil {
				fmt.Println("  ❌ Error:", err)
				continue
			}
			fmt.Printf("  ✅ Key %s deleted successfully\n", cmd.Key)

		case parser.CmdExists:
			exists, err := store.Exists(cmd.Key)
			if err != nil {
				fmt.Println("  ❌ Error:", err)
				continue
			}
			
			if exists {
				fmt.Printf("  ✅ Key %s exists\n", cmd.Key)
			} else {
				fmt.Printf("  ❌ Key %s does not exist\n", cmd.Key)
			}
			
		case parser.CmdType:
			valueType, err := store.Type(cmd.Key)
			if err != nil {
				fmt.Println("  ❌ Error:", err)
				continue
			}
			fmt.Printf("  ✅ Key %s has type %s\n", cmd.Key, valueType)

		case parser.CmdAdd:
			value := engine.Value{
				Data:      cmd.Value,
				ValueType: cmd.ValueType,
			}
			if err := store.Add(cmd.Key, value); err != nil {
				fmt.Println("  ❌ Error:", err)
				continue
			}
			fmt.Printf("  ✅ Added %s to %s\n", cmd.Value, cmd.Key)

		case parser.CmdSub:
			value := engine.Value{
				Data:      cmd.Value,
				ValueType: cmd.ValueType,
			}
			if err := store.Sub(cmd.Key, value); err != nil {
				fmt.Println("  ❌ Error:", err)
				continue
			}
			fmt.Printf("  ✅ Subtracted %s from %s\n", cmd.Value, cmd.Key)

		case parser.CmdMul:
			value := engine.Value{
				Data:      cmd.Value,
				ValueType: cmd.ValueType,
			}
			if err := store.Mul(cmd.Key, value); err != nil {
				fmt.Println("  ❌ Error:", err)
				continue
			}
			fmt.Printf("  ✅ Multiplied %s by %s\n", cmd.Key, cmd.Value)
			
		case parser.CmdDiv:
			value := engine.Value{
				Data:      cmd.Value,
				ValueType: cmd.ValueType,
			}
			
			if err := store.Div(cmd.Key, value); err != nil {
				fmt.Println("  ❌ Error:", err)
				continue
			}
			fmt.Printf("  ✅ Divided %s by %s\n", cmd.Key, cmd.Value)
			
		default:
			fmt.Println("  ❌ Unknown command type:", cmd.Type)
		}
	}
}