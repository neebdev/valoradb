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
				Data:      cmd.Value,
				ValueType: cmd.ValueType,
			}
			store.Set(cmd.Key, value)
		case parser.CmdGet:
			val, _ := store.Get(cmd.Key)
			fmt.Println(val.Data)

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
