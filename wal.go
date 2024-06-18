package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"
)

const (
	blockSize            = 16
	createLogPermission  = os.O_APPEND | os.O_CREATE | os.O_RDWR
	recoverLogPermission = os.O_RDWR
)

type LogEntry struct {
	Offset    int
	Length    uint32
	Data      []byte
	Checksum  uint32
	Timestamp int64
	Index     int64
}

type WAL struct {
	mu           *sync.RWMutex
	option       *WALOption
	segmentIndex int
	segmentFile  uint16
	segments     []*Segment
	buffer       *WALBuffer
	lru          *lru.Cache
	pos          int
}

type WALBuffer struct {
	buf *bytes.Buffer
	mu  sync.Mutex
}

func (b *WALBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(data)
}

func (b *WALBuffer) Flush(w io.Writer) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, err := b.buf.WriteTo(w)
	b.buf.Reset()
	return int(n), err
}

func (b *WALBuffer) ReadFromBuffer() *bufio.Reader {
	b.mu.Lock()
	defer b.mu.Unlock()

	return bufio.NewReader(bytes.NewReader(b.buf.Bytes()))
}

func New(opts ...WALOpt) (*WAL, error) {
	wal := &WAL{
		mu:     new(sync.RWMutex),
		option: DefaultWalOption,
		buffer: &WALBuffer{
			buf: &bytes.Buffer{},
			mu:  sync.Mutex{},
		},
	}

	for _, o := range opts {
		o(wal.option)
	}

	cache, err := lru.New(wal.option.cacheSize)
	if err != nil {
		return nil, err
	}
	wal.lru = cache

	err = os.MkdirAll(wal.option.dir, 0755)
	if err != nil {
		return nil, err
	}

	err = wal.LoadSegments()
	if err != nil {

		return nil, err
	}

	if len(wal.segments) == 0 {
		err = wal.createSegment()
		if err != nil {
			return nil, err
		}
	}

	return wal, nil
}

func (w *WAL) Write(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := make([]byte, blockSize+len(data))

	timestamp := time.Now().UnixNano()
	binary.BigEndian.PutUint64(entry[0:8], uint64(timestamp))

	length := uint32(len(data))
	binary.BigEndian.PutUint32(entry[8:12], length)

	checksum := crc32.ChecksumIEEE(data)
	binary.BigEndian.PutUint32(entry[12:16], checksum)

	w.pos++

	copy(entry[16:], data)

	seg := w.CurrentSegment()
	seg.offset = append(seg.offset, pos{
		offset: seg.size,
		length: length,
	})
	seg.size += int64(blockSize + length)

	_, err := w.buffer.Write(entry)
	if err != nil {
		return err
	}

	if w.buffer.buf.Len() >= int(w.option.maxWriteBufferSize) {
		if err := w.flushBuffer(); err != nil {
			return err
		}
	}

	return nil
}

func (w *WAL) Iter(callback func(index int, entry *LogEntry) bool) error {
	for _, seg := range w.segments {
		var (
			index = 0
			stop  = false
		)

		for index < seg.Size() {
			offset := seg.offset[index]
			var entry *LogEntry

			if seg.OnActiveBuffer(index) {
				byt := w.buffer.buf.Bytes()[offset.offset-seg.currSize : offset.EndOffset()-seg.currSize]
				buf := bytes.NewReader(byt)
				reader := bufio.NewReader(buf)

				logEntry, err := seg.ReadEntry(reader)
				if err != nil {
					return err
				}

				entry = logEntry
			} else {
				logEntry, err := seg.SeekOffset(offset.offset)
				if err != nil {
					if err == io.EOF {
						break
					}

					return err
				}
				entry = logEntry
			}

			next := callback(index, entry)
			if !next {
				stop = true
				break
			}

			index++
		}

		if stop {
			break
		}
	}

	return nil
}

func (w *WAL) IterReverse(callback func(index int, entry *LogEntry) bool) error {
	for i := len(w.segments) - 1; i >= 0; i-- {
		var (
			seg   = w.segments[i]
			index = seg.Size() - 1
			stop  = false
		)

		for index >= 0 {
			offset := seg.offset[index]
			var entry *LogEntry

			if seg.OnActiveBuffer(index) {
				byt := w.buffer.buf.Bytes()[offset.offset-seg.currSize : offset.EndOffset()-seg.currSize]
				buf := bytes.NewReader(byt)
				reader := bufio.NewReader(buf)

				logEntry, err := seg.ReadEntry(reader)
				if err != nil {
					return err
				}

				entry = logEntry
			} else {
				logEntry, err := seg.SeekOffset(offset.offset)
				if err != nil {
					if err == io.EOF {
						break
					}

					return err
				}
				entry = logEntry
			}

			next := callback(index, entry)
			if !next {
				stop = true
				break
			}

			index--
		}

		if stop {
			break
		}
	}

	return nil
}

// Read retrieves data based on the index.
// Note: The index will reset if the segment file reaches the maximum limit.
func (w *WAL) ReadIndex(index int) (entry *LogEntry, err error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if entry, ok := w.lru.Get(index); ok {
		return entry.(*LogEntry), nil
	}

	var (
		currOffset      = 0
		nextOffset      = 0
		found      bool = false
	)

	defer func() {
		if found {
			w.lru.Add(index, entry)
		}
	}()

	for _, seg := range w.segments {
		nextOffset = nextOffset + len(seg.offset)
		if index >= currOffset && index < nextOffset {
			currIndex := index - currOffset
			offset := seg.offset[currIndex]
			if w.ActiveSegmentIndex() == seg.index && seg.OnActiveBuffer(currIndex) {
				byt := w.buffer.buf.Bytes()[offset.offset-seg.currSize : offset.EndOffset()-seg.currSize]
				buf := bytes.NewReader(byt)
				reader := bufio.NewReader(buf)
				entry, err = seg.ReadEntry(reader)
				if err == nil {
					found = true
					return
				}
			}

			entry, err = seg.SeekOffset(offset.offset)
			if err == nil {
				found = true
			}

			return
		} else {
			currOffset = nextOffset
		}
	}

	return nil, fmt.Errorf("entry with index %d not found", index)
}

func (w *WAL) LoadSegments() error {
	return filepath.Walk(w.option.dir, func(path string, info fs.FileInfo, err error) error {
		if !info.IsDir() && filepath.Ext(path) == ".log" {
			segFile, err := os.OpenFile(path, createLogPermission, 0644)
			if err != nil {
				return err
			}

			name := strings.Split(info.Name()[:len(info.Name())-len(filepath.Ext(path))], "-")
			if currIndex, err := strconv.ParseInt(name[len(name)-1], 10, 64); err == nil {
				w.segmentIndex = int(currIndex)
			}

			segment := &Segment{
				index:  w.segmentIndex,
				path:   path,
				fd:     segFile,
				writer: bufio.NewWriter(segFile),
				size:   info.Size(),
			}

			err = segment.Read()
			if err != nil {
				return err
			}

			w.segments = append(w.segments, segment)
			w.segmentIndex++
			w.segmentFile++
			w.pos += len(segment.offset)
		}

		return nil
	})
}

func (w *WAL) ActiveSegmentIndex() int {
	return w.segments[len(w.segments)-1].index
}

func (w *WAL) CurrentSegment() *Segment {
	return w.segments[len(w.segments)-1]
}

func (w *WAL) CurrentPosition() int {
	return w.pos - 1
}

// deleteSegments: removes all segments if the maximum number of segment files has been reached.
func (w *WAL) deleteSegments() error {
	for _, seg := range w.segments {
		_ = seg.fd.Close()

		err := os.Remove(seg.path)
		if err != nil {
			return err
		}
	}

	w.segments = make([]*Segment, 0)
	w.pos = 0

	return nil
}

// deleteSegments: removes all segments if the maximum number of segment files has been reached.
func (w *WAL) Delete() error {
	for _, seg := range w.segments {
		err := seg.fd.Close()
		if err != nil {
			return err
		}

		err = os.Remove(seg.path)
		if err != nil {
			return err
		}
	}

	w.segments = make([]*Segment, 0)
	w.pos = 0

	return nil
}

func (w *WAL) Sync() error {
	curr := w.CurrentSegment()
	err := curr.writer.Flush()
	if err != nil {
		return err
	}

	err = curr.fd.Sync()
	if err != nil {
		return err
	}

	stat, err := curr.fd.Stat()
	if err != nil {
		return err
	}

	curr.currSize = stat.Size()

	return nil
}

func (wal *WAL) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if err := wal.flushBuffer(); err != nil {
		return err
	}

	for _, segment := range wal.segments {
		if err := segment.writer.Flush(); err != nil {
			return err
		}
		if err := segment.fd.Sync(); err != nil {
			return err
		}

		if err := segment.fd.Close(); err != nil {
			return err
		}
	}

	return nil
}

// flushBuffer: If the buffer reaches its maximum size, it will be synced to the file and then reset.
func (w *WAL) flushBuffer() error {
	curr := w.CurrentSegment()

	_, err := w.buffer.Flush(curr.writer)
	if err != nil {
		return err
	}

	err = w.Sync()
	if err != nil {
		return err
	}

	if curr.size >= w.option.maxSegmentSize {
		if len(w.segments)+1 > int(w.option.maxSegmentFile) {
			err = w.deleteSegments()
			if err != nil {
				return err
			}
		}

		err = w.createSegment()
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *WAL) createSegment() error {
	segPath := filepath.Join(w.option.dir, fmt.Sprintf("%s-%06d.log", w.option.prefix, w.segmentIndex))
	segment, err := os.OpenFile(segPath, createLogPermission, 0644)
	if err != nil {
		return err
	}

	w.segments = append(w.segments, &Segment{
		index:  w.segmentIndex,
		path:   segPath,
		size:   0,
		fd:     segment,
		writer: bufio.NewWriter(segment),
		offset: make([]pos, 0),
	})
	w.segmentIndex++
	w.segmentFile++

	return nil
}
