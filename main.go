package main

import (
	"fmt"
	"os"

	"github.com/neebdev/valoradb/internal/engine"
	"github.com/neebdev/valoradb/internal/parser"
)

func main() {
	queries, err := parser.ParseQueriesFromFile("queries.txt")
	if err != nil {
		fmt.Println("Error reading queries:", err)
		return
	}

	walPath := "./wal.log"
	walFile, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("failed to create wal file: %v", err)
	}
	defer walFile.Close()
	store := &engine.Store{
		Data: make(map[string]engine.Value),
		Wal:  walFile,
	}

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
						Data: cmd.Value,
						ValueType: cmd.ValueType,
					}
					store.Set(cmd.Key, value)
			case parser.CmdGet:
					val, _ := store.Get(cmd.Key)
					fmt.Println(val.Data)
			case parser.CmdKeys:
					val, _ := store.Keys(cmd.Value)
					fmt.Println(val)
			case parser.CmdDel:
				err := store.Del(cmd.Key)
				if err != nil {
					fmt.Println("  ❌ Error:", err)
				} else {
					fmt.Printf("%v deleted successfully!", cmd.Key)
				}

			case parser.CmdExists:
				exists, err := store.Exists(cmd.Key)
				if err != nil {
					fmt.Println("  ❌ Error:", err)
				} else {
					if exists {
						fmt.Printf("✅ Key '%s' exists!\n", cmd.Key)
					} else {
						fmt.Printf("❌ Key '%s' does not exist.\n", cmd.Key)
					}
				}
			case parser.CmdType:
				valueType, err := store.Type(cmd.Key)
				if err != nil {
					fmt.Println("  ❌ Error:", err)
				} else {
					fmt.Printf("📦 Key '%s' has type '%s'\n", cmd.Key, valueType)
				}

			case parser.CmdAdd:
				value := engine.Value{
					Data:      cmd.Value,
					ValueType: cmd.ValueType,
				}
				store.Add(cmd.Key, value)

			case parser.CmdSub:
				value := engine.Value{
					Data:      cmd.Value,
					ValueType: cmd.ValueType,
				}
				store.Sub(cmd.Key, value)

			case parser.CmdMul:
				value := engine.Value{
					Data:      cmd.Value,
					ValueType: cmd.ValueType,
				}
				store.Mul(cmd.Key, value)
			case parser.CmdDiv:
				value := engine.Value{
					Data:      cmd.Value,
					ValueType: cmd.ValueType,
				}
				store.Div(cmd.Key, value)
		}
	}
}