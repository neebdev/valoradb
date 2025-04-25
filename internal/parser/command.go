package parser

type CommandType string

const (
	CmdSet      CommandType = "SET"
	CmdGet      CommandType = "GET"
	CmdDel      CommandType = "DEL"
	CmdBegin    CommandType = "BEGIN"
	CmdCommit   CommandType = "COMMIT"
	CmdRollback CommandType = "ROLLBACK"
	CmdAdd      CommandType = "ADD"
	CmdSub      CommandType = "SUB"
	CmdExists   CommandType = "EXISTS"
	CmdKeys     CommandType = "KEYS"
	CmdType     CommandType = "TYPE"
)

type ValueType string

const (
	TypeString ValueType = "string"
	TypeNumber ValueType = "number"
	TypeBool   ValueType = "bool"
)

type Command struct {
	Type      CommandType
	Key       string
	Value     string
	ValueType ValueType
	Raw       string
}
