# MapReduce Implementation

A Go-based implementation of the MapReduce framework described in Google's seminal [MapReduce paper](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf) (OSDI 2004). This project is a solution for the MIT 6.824 Distributed Systems course lab.

## Overview

MapReduce is a programming model for processing large data sets with a parallel, distributed algorithm on a cluster. This implementation provides a simplified but functional version of the MapReduce framework that can:
- Distribute map and reduce tasks across multiple workers
- Handle worker failures with task re-assignment
- Coordinate parallel execution through a central coordinator
- Process text data using custom map and reduce functions

## Project Structure

```
.
├── mr/                    # MapReduce framework implementation (custom solution)
│   ├── coordinator.go     # Coordinator/master node logic
│   ├── worker.go          # Worker node implementation
│   └── rpc.go             # RPC definitions for worker-coordinator communication
├── main/                  # Entry points and test files (provided by MIT)
│   ├── mrcoordinator.go   # Starts the coordinator process
│   ├── mrworker.go        # Starts a worker process
│   ├── mrsequential.go    # Sequential (non-distributed) MapReduce for testing
│   ├── test-mr.sh         # Basic test suite
│   ├── test-mr-many.sh    # Extended test suite for stress testing
│   └── pg-*.txt           # Sample input files (Project Gutenberg texts)
├── mrapps/                # MapReduce applications (provided by MIT)
│   ├── wc.go              # Word count application
│   ├── indexer.go         # Inverted index generator
│   ├── crash.go           # Tests crash recovery
│   ├── early_exit.go      # Tests early worker termination
│   └── ...
└── go.mod                 # Go module definition
```

**Note:** The `main/` and `mrapps/` directories contain code provided by the MIT course. The `mr/` directory contains the custom implementation of the MapReduce framework.

## Architecture

### Coordinator
The coordinator (master node) is responsible for:
- Task distribution and scheduling
- Tracking task states (idle, in-progress, completed)
- Worker health monitoring
- Handling worker failures and task reassignment
- Coordinating the transition from map phase to reduce phase

### Workers
Workers are responsible for:
- Requesting tasks from the coordinator via RPC
- Executing map and reduce functions on assigned data
- Writing intermediate and final output files
- Reporting task completion to the coordinator

### Task Flow
1. **Map Phase**: Workers read input files, apply the map function, and partition intermediate key-value pairs into R buckets (one per reduce task)
2. **Reduce Phase**: Workers read intermediate files, sort by key, apply the reduce function, and write final output
3. **Completion**: Coordinator signals completion when all reduce tasks finish

## Building and Running

### Prerequisites
- Go 1.21 or later

### Build MapReduce Applications
```bash
cd main
go build -buildmode=plugin ../mrapps/wc.go
```

### Run Word Count Example

**Terminal 1 - Start the Coordinator:**
```bash
cd main
go run mrcoordinator.go pg-*.txt
```

**Terminal 2 (and 3, 4...) - Start Workers:**
```bash
cd main
go run mrworker.go wc.so
```

The output files `mr-out-*` will contain the final reduced results.

### Run Sequential Version (for testing)
```bash
cd main
go run mrsequential.go wc.so pg-*.txt
cat mr-out-0
```

## Testing

### Basic Tests
```bash
cd main
bash test-mr.sh
```

### Stress Tests
```bash
cd main
bash test-mr-many.sh
```

The tests verify:
- Correct output for word count and indexer applications
- Handling of worker crashes and failures
- Parallel execution correctness
- Race condition detection

## MapReduce Applications

### Word Count (`wc.go`)
Counts the occurrences of each word across all input files.
- **Map**: Emits (word, "1") for each word
- **Reduce**: Sums all counts for each word

### Indexer (`indexer.go`)
Creates an inverted index mapping words to documents.
- **Map**: Emits (word, filename) for each word
- **Reduce**: Aggregates all filenames for each word

### Test Applications
- `crash.go`: Randomly crashes to test fault tolerance
- `early_exit.go`: Tests early worker termination
- `mtiming.go`, `rtiming.go`: Test timing and performance
- `nocrash.go`, `jobcount.go`: Additional testing utilities

## Implementation Highlights

### Fault Tolerance
- Workers that fail to complete tasks within a timeout are detected
- Failed tasks are automatically reassigned to other workers
- The system continues to make progress even with worker failures

### Concurrency
- Multiple workers can execute tasks in parallel
- Thread-safe coordinator using mutex locks
- RPC-based communication between coordinator and workers

### Data Partitioning
- Intermediate data is partitioned using hash(key) % nReduce
- Each reduce task processes one partition across all map outputs
- Efficient disk I/O using JSON encoding for intermediate files

## Key Files

- **`mr/coordinator.go`**: Implements the Coordinator struct and task management logic
- **`mr/worker.go`**: Implements the Worker main loop and map/reduce execution
- **`mr/rpc.go`**: Defines RPC request/response structures

## References

- [MapReduce: Simplified Data Processing on Large Clusters](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf) - Original Google paper
- [MIT 6.824 Distributed Systems](https://pdos.csail.mit.edu/6.824/) - Course website

## License
This is an educational project for the MIT 6.824 course. The framework implementation in `mr/` is custom code, while `main/` and `mrapps/` are provided by MIT.
