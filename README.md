# ValoraDB

A simple in-memory database with transaction support and write-ahead logging.

## Configuration

ValoraDB uses a simple JSON configuration file to manage its settings. The configuration file is named `config.json` and should be located in the same directory as the application executable.

### Configuration Options

- `wal`: WAL-specific configuration
  - `directory`: Directory where WAL files are stored (default: `./data/wal`)
  - `segmentSize`: Maximum size of each WAL segment in bytes (default: 16MB)

### Example Configuration

```json
{
  "wal": {
    "directory": "./data/wal",
    "segmentSize": 16777216
  }
}
```

## Running ValoraDB

To start the database:

```
go run .
```

Or if you've built the binary:

```
./valoradb
```

## Commands

ValoraDB supports the following commands:

- `SET key value` - Set a key to a value
- `GET key` - Get the value of a key
- `DEL key` - Delete a key
- `EXISTS key` - Check if a key exists
- `TYPE key` - Get the type of a key's value
- `KEYS pattern` - Find keys matching a pattern
- `BEGIN` - Start a transaction
- `COMMIT` - Commit a transaction
- `ROLLBACK` - Rollback a transaction
- `ADD key value` - Add a value to a key (numeric only)
- `SUB key value` - Subtract a value from a key (numeric only)
- `MUL key value` - Multiply a key by a value (numeric only)
- `DIV key value` - Divide a key by a value (numeric only)
- `CLEAR` - Clear all keys

## Example

```
SET counter 10
ADD counter 5
GET counter  # Returns 15
BEGIN
SUB counter 2
GET counter  # Returns 13
ROLLBACK
GET counter  # Returns 15
```
