package wal

import (
	"log"
	"os"
)

type WALOption struct {
	cacheSize          int
	dir                string
	janitorHook        func(segment *Segment)
	maxFileLifetime    int8
	maxSegmentFile     uint16
	maxSegmentSize     int64
	maxWriteBufferSize int64
	prefix             string
}

var DefaultWalOption = &WALOption{
	prefix:             "wal",
	dir:                "/var/logs/",
	maxSegmentSize:     20 * 1024 * 1024,   // 20 MB (log rotation size)
	maxSegmentFile:     10,                 // maximum number of segment files or (-1 or 0) for keep the logs
	maxWriteBufferSize: 32 * 1024,          // 32KB buffer size
	cacheSize:          10 * 1024 * 1024,   // 10 MB (cache size)
	maxFileLifetime:    0,                  // No janitor to cleanup the logs file
	janitorHook:        DefaultJanitorHook, // The default behavior is to remove the segment files. For customization, you can add an alternative method.
}

// Remove the segment file
var DefaultJanitorHook = func(seg *Segment) {
	_ = seg.fd.Close()

	err := os.Remove(seg.path)
	if err != nil {
		log.Printf("error remove segment %s cause error %s\n", seg.path, err.Error())
	}
}

type WALOpt func(opt *WALOption)

func WithDir(dir string) WALOpt {
	return func(opt *WALOption) {
		opt.dir = dir
	}
}

func WithPrefix(prefix string) WALOpt {
	return func(opt *WALOption) {
		opt.prefix = prefix
	}
}

func WithMaxSegmentSize(size int64) WALOpt {
	return func(opt *WALOption) {
		opt.maxSegmentSize = size
	}
}

func WithMaxSegmentFile(size uint16) WALOpt {
	return func(opt *WALOption) {
		opt.maxSegmentFile = size
	}
}

func WithMaxWriteBufferSize(size int64) WALOpt {
	return func(opt *WALOption) {
		if size > 64*1024 {
			size = 64 * 1024
		}

		opt.maxWriteBufferSize = size
	}
}

func WithMaxFileLifetime(lifetime int8) WALOpt {
	return func(opt *WALOption) {
		opt.maxFileLifetime = lifetime
	}
}

func WithCustomJanitorHook(hook func(seg *Segment)) WALOpt {
	return func(opt *WALOption) {
		opt.janitorHook = hook
	}
}
