package parser

type CommandType string

const (
	CmdSet    CommandType = "SET"
	CmdGet    CommandType = "GET"
	CmdDel    CommandType = "DEL"
	CmdBegin  CommandType = "BEGIN"
	CmdCommit CommandType = "COMMIT"
	CmdAbort  CommandType = "ABORT"
	CmdAdd    CommandType = "ADD"
	CmdSub    CommandType = "SUB"
)

type ValueType string

const (
	TypeString ValueType = "string"
	TypeInt    ValueType = "int"
	TypeFloat  ValueType = "float"
	TypeBool   ValueType = "bool"
)

type Command struct {
	Type      CommandType
	Key       string
	Value     string
	ValueType ValueType // Optional: inferred or explicit
	Raw       string    // string for logging/debugging
}
