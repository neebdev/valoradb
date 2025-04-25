package main

import (
	"fmt"

	"github.com/neebdev/valoradb/internal/parser"
)

func main() {
	queries, err := parser.ParseQueriesFromFile("queries.txt")
	if err != nil {
		fmt.Println("Error reading queries:", err)
		return
	}

	for _, rawQuery := range queries {
		fmt.Println(">", rawQuery)

		cmd, err := parser.ParseCommand(rawQuery)
		if err != nil {
			fmt.Println("  ❌ Error:", err)
			continue
		}

		fmt.Printf("  ✅ Parsed: %+v\n", cmd)
	}
}