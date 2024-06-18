## Write Ahead Log

This package provides a robust Write-Ahead Log (WAL) implementation in Go. It ensures data integrity by writing logs to persistent storage before applying them to the system. This implementation is suitable for applications requiring high reliability and consistency, such as databases and distributed systems.

### Features
- **Concurrency Support**: Uses sync.RWMutex to manage concurrent read/write access.
- **Buffered Writes**: Implements a buffered write mechanism to optimize disk I/O.
- **Segment Management**: Supports segment-based log rotation and management.
- **LRU Cache**: Integrates an LRU cache to improve read performance.
- **Checksum Validation**: Ensures data integrity using CRC32 checksums.
- **Configurable Options**: Provides configurable options for log directory, segment size, buffer size, and more.

### Configuration Options
You can configure the WAL instance using the following options:

- ```WithDir(dir string)```: Sets the directory for log files.
- ```WithPrefix(prefix string)```: Sets the prefix for log file names.
- ```WithMaxSegmentSize(size int64)```: Sets the maximum size of a log segment.
- ```WithMaxSegmentFile(size uint16)```: Sets the maximum number of segment files.
- ```WithMaxWriteBufferSize(size int64)```: Sets the maximum size of the write buffer.

### Installation

To install the package, use the following command:

```bash
go get github.com/yudhasubki/wal
```

### Usage
#### Creating a WAL Instance
To create a new WAL instance, use the New function with optional configuration parameters:

```go
package main

import (
    "github.com/yudhasubki/wal"
)

func main() {
    logInstance, err := wal.New(
		wal.WithDir("./logs"),
		wal.WithPrefix("examples-wal"),
		wal.WithMaxSegmentSize(5*1024*1024), //  1MB (Log rotation size)
		wal.WithMaxSegmentFile(5),           // maximum number of segment files
	)
	if err != nil {
		panic(err)
	}
	defer logInstance.Close()
}
```

#### Writing to the WAL Instance 
To write data to the WAL, use the ```Write``` method:

```go
data := []byte("your data")
err := logInstance.Write(data)
if err != nil {
    log.Fatalf("Failed to write to WAL: %v", err)
}
```

#### Reading from the WAL 

To read data from the WAL based on the index, use the ```ReadIndex``` method:

```go
index := 0
entry, err := logInstance.ReadIndex(index)
if err != nil {
    log.Fatalf("Failed to read from WAL: %v", err)
}
fmt.Printf("Read entry: %s\n", entry.Data)
```

#### Iterating Over WAL Entries
To iterate over all entries in the WAL, use the ```Iter``` method:

```go
err := logInstance.Iter(func(index int, entry *wal.LogEntry) bool {
    fmt.Printf("Index: %d, Entry: %s\n", index, entry.Data)
    return true // continue iteration
})
if err != nil {
    log.Fatalf("Failed to iterate over WAL: %v", err)
}
```

#### Iterating From Tail Over WAL Entries
To iterate from tail over all entries in the WAL, use the ```IterReverse``` method:

```go
err := logInstance.IterReverse(func(index int, entry *wal.LogEntry) bool {
    fmt.Printf("Index: %d, Entry: %s\n", index, entry.Data)
    return true // continue iteration
})
if err != nil {
    log.Fatalf("Failed to iterate over WAL: %v", err)
}
```

#### Flushing the Buffer 
To flush the buffer and ensure all data is written to disk, use the ```Sync``` method:

```go
err := logInstance.Sync()
if err != nil {
    log.Fatalf("Failed to sync WAL: %v", err)
}
```

### License
This project is licensed under the MIT License. See the LICENSE file for details.

### Contributions
Contributions are welcome! Please open an issue or submit a pull request on GitHub.

