# Write-Ahead Log (WAL) Implementation

This directory contains the implementation of a Write-Ahead Log (WAL) system for data persistence in distributed systems. The WAL ensures data durability and recovery in the face of system failures.

## Architecture

The implementation consists of several components that work together:

### Core Interfaces

1. **DataPersistenceInterface**

   - Defines the contract for data persistence operations
   - Methods: `persist`, `retrieve`, `clear`

2. **StorageInterface**

   - Abstracts storage operations (file system, database, etc.)
   - Provides methods for reading/writing data

3. **StateInterpreterInterface**
   - Handles formatting, parsing, and merging of data
   - Makes data storage format independent from business logic

### Implementations

1. **WriteAheadLog**

   - Main implementation of `DataPersistenceInterface`
   - Uses log files for durability and checkpoints for efficiency
   - Handles recovery from system failures

2. **FileSystemStorage**

   - Implementation of `StorageInterface` using local file system
   - Handles all file operations with appropriate error handling

3. **ConsumerStateInterpreter**
   - Implementation of `StateInterpreterInterface` for consumer data
   - Formats/parses data specific to consumer operations

## How it Works

The WAL operates with the following workflow:

1. **Log Writing**:

   - When data needs to be persisted, it's first formatted by the StateInterpreter
   - Data is written to a log file with a unique operation ID
   - Status is marked as "PROCESSING" during write and "COMPLETED" after successful write

2. **Checkpointing**:

   - After a fixed number of logs are written (default: 3)
   - Logs are consolidated into a single checkpoint file
   - Individual log files are removed after successful checkpoint

3. **Recovery**:

   - On startup, the system reads the latest checkpoint and subsequent logs
   - Data is merged to reconstruct the most up-to-date state
   - Partial/incomplete logs are ignored

4. **Cleanup**:
   - Data can be explicitly cleared when no longer needed

## Usage Example

```python
# Initialize components
storage = FileSystemStorage()
state_interpreter = ConsumerStateInterpreter()

# Create WAL instance
wal = WriteAheadLog(
    state_interpreter=state_interpreter,
    storage=storage,
    service_name="my_service",
    base_dir="/app/persistence"
)

# Persist data
client_id = "client123"
data = {"batch": 5, "timestamp": "1621234567", "value": "some data"}
batch_id = "5"  # Use batch ID or any external identifier
wal.persist(client_id, data, batch_id)  # WAL generates internal operation IDs

# Retrieve data
retrieved_data = wal.retrieve(client_id)

# Clear data when no longer needed
wal.clear(client_id)
```

## Fault Tolerance Features

- **Durability**: Data is safely written to persistent storage
- **Atomicity**: Operations are either complete or not visible
- **Recoverability**: System state can be reconstructed after crashes
- **Checkpointing**: Regular consolidation improves efficiency and recovery speed
