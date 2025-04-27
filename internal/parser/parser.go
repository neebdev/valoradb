package parser

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func ParseQueriesFromFile(filename string) ([]string, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}

	content := string(data)
	rawQueries := strings.Split(content, ";")

	var queries []string
	for _, raw := range rawQueries {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			continue
		}
		
		// Remove SQL-style comments (-- comment)
		lines := strings.Split(trimmed, "\n")
		var cleanedLines []string
		for _, line := range lines {
			// If line starts with comment, skip it
			if strings.HasPrefix(strings.TrimSpace(line), "--") {
				continue
			}
			// If line contains comment, remove it
			if idx := strings.Index(line, "--"); idx >= 0 {
				line = line[:idx]
			}
			if strings.TrimSpace(line) != "" {
				cleanedLines = append(cleanedLines, line)
			}
		}
		
		cleanedQuery := strings.Join(cleanedLines, " ")
		if cleanedQuery != "" {
			// Preserve the original format without normalizing
			queries = append(queries, cleanedQuery)
		}
	}
	return queries, nil
}

func ParseCommand(raw string) (*Command, error) {
	// Use custom tokenization to properly handle quoted strings
	tokens := tokenize(raw)
	if len(tokens) == 0 {
		return nil, errors.New("empty command")
	}

	cmd := &Command{
		Type: CommandType(strings.ToUpper(tokens[0])),
		Raw:  raw,
	}

	switch cmd.Type {
	case CmdSet:
		if len(tokens) < 3 {
			return nil, errors.New("SET must be: SET key value [TYPE type]")
		}
		cmd.Key = tokens[1]
		cmd.Value = tokens[2]

		if len(tokens) >= 5 && strings.ToUpper(tokens[3]) == "TYPE" {
			cmd.ValueType = ValueType(strings.ToLower(tokens[4]))
		}

	case CmdAdd, CmdSub, CmdDiv, CmdMul:
		if len(tokens) != 3 {
			return nil, fmt.Errorf("%s must be: %s key value", cmd.Type, cmd.Type)
		}
		cmd.Key = tokens[1]
		cmd.Value = tokens[2]

	case CmdGet, CmdDel:
		if len(tokens) != 2 {
			return nil, fmt.Errorf("%s must be: %s key", cmd.Type, cmd.Type)
		}
		cmd.Key = tokens[1]

	case CmdExists:
		if len(tokens) != 2 {
			return nil, fmt.Errorf("%s must be: %s key", cmd.Type, cmd.Type)
		}
		cmd.Key = tokens[1]

	case CmdKeys:
		if len(tokens) != 2 {
			return nil, fmt.Errorf("%s must be: %s pattern", cmd.Type, cmd.Type)
		}
		cmd.Value = tokens[1]

	case CmdType:
		if len(tokens) != 2 {
			return nil, fmt.Errorf("%s must be: %s key", cmd.Type, cmd.Type)
		}
		cmd.Key = tokens[1]

	case CmdBegin, CmdCommit, CmdRollback, CmdClear:
		// These commands don't require additional arguments

	default:
		return nil, errors.New("unknown command: " + string(cmd.Type))
	}

	err := ValidateCommand(cmd)
	if err != nil {
		return nil, err
	}

	return cmd, nil
}

// tokenize splits a command into tokens while preserving quoted strings
func tokenize(s string) []string {
	var tokens []string
	var current strings.Builder
	inQuotes := false
	
	// Helper to add token when we encounter a delimiter
	addToken := func() {
		if current.Len() > 0 {
			tokens = append(tokens, current.String())
			current.Reset()
		}
	}
	
	for i := 0; i < len(s); i++ {
		ch := s[i]
		
		// Handle quotes
		if ch == '"' {
			current.WriteByte(ch)
			inQuotes = !inQuotes
			continue
		}
		
		// Handle spaces
		if ch == ' ' && !inQuotes {
			addToken()
			continue
		}
		
		// Add character to current token
		current.WriteByte(ch)
	}
	
	// Add the last token if any
	addToken()
	
	return tokens
}

func InferType(value string) (ValueType, error) {
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		return TypeNumber, nil
	}
	if value == "true" || value == "false" {
		return TypeBool, nil
	}
	if strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"") {
		return TypeString, nil
	}
	return "", fmt.Errorf("could not infer type from value: %s", value)
}

func ValidateCommand(cmd *Command) error {
	usesValue := cmd.Type == CmdSet || cmd.Type == CmdAdd || cmd.Type == CmdSub || cmd.Type == CmdDiv || cmd.Type == CmdMul
	if !usesValue {
		return nil
	}

	inferredType, err := InferType(cmd.Value)
	if err != nil {
		return fmt.Errorf("invalid value: %s", cmd.Value)
	}

	if cmd.ValueType == "" {
		cmd.ValueType = inferredType
	} else {
		if inferredType != cmd.ValueType {
			return fmt.Errorf("type mismatch: value '%s' inferred as %s, but declared as %s", cmd.Value, inferredType, cmd.ValueType)
		}
	}

	if cmd.ValueType == TypeString {
		cmd.Value = strings.Trim(cmd.Value, "\"")
	}

	switch cmd.Type {
	case CmdAdd, CmdSub, CmdMul:
		if cmd.ValueType != TypeNumber {
			return fmt.Errorf("%s supports only int or float types, not %s", cmd.Type, cmd.ValueType)
		}
	case CmdSet:
		if cmd.ValueType != TypeNumber && cmd.ValueType != TypeBool && cmd.ValueType != TypeString {
			return fmt.Errorf("SET supports only int, float, bool, string types, not %s", cmd.ValueType)
		}
	case CmdDiv:
		if cmd.ValueType != TypeNumber {
			return fmt.Errorf("%s supports only int or float types, not %s", cmd.Type, cmd.ValueType)
		}
		// We'll check for division by zero in the actual DIV operation,
		// not during command parsing
	}

	return nil
}
