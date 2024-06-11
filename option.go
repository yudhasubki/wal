package wal

type WALOption struct {
	prefix             string
	dir                string
	maxSegmentSize     int64
	maxWriteBufferSize int64
	maxSegmentFile     uint16
	cacheSize          int
}

var DefaultWalOption = &WALOption{
	prefix:             "wal",
	dir:                "/var/logs/",
	maxSegmentSize:     20 * 1024 * 1024, // 20 MB (log rotation size)
	maxSegmentFile:     10,               // maximum number of segment files
	maxWriteBufferSize: 1 * 1024 * 1024,
	cacheSize:          10 * 1024 * 1024, // 10 MB (cache size)
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
