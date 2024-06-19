package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"
)

// Segment represents a signgle segment file
type Segment struct {
	index    int
	path     string
	size     int64
	fd       *os.File
	writer   *bufio.Writer
	offset   []pos
	currSize int64
	modTime  time.Time
}

func (s *Segment) OnActiveBuffer(idx int) bool {
	return s.offset[idx].offset > s.currSize || s.currSize == 0
}

type pos struct {
	offset int64
	length uint32
}

func (p *pos) EndOffset() int64 {
	return p.offset + blockSize + int64(p.length)
}

func (s *Segment) Read() error {
	var (
		offset int64
		reader = bufio.NewReader(s.fd)
	)

	for {
		entry, err := s.ReadEntry(reader)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		calcChecksum := crc32.ChecksumIEEE(entry.Data)
		if entry.Checksum != calcChecksum {
			return fmt.Errorf("checksum mismatch at offset %d", offset)
		}

		s.offset = append(s.offset, pos{
			offset: offset,
			length: entry.Length,
		})

		offset += int64(entry.Offset)
	}

	s.size = offset
	_, err := s.fd.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}
	s.writer = bufio.NewWriter(s.fd)

	return nil
}

func (s *Segment) ReadEntry(reader *bufio.Reader) (*LogEntry, error) {
	header := make([]byte, 16)
	_, err := io.ReadFull(reader, header)
	if err != nil {
		return nil, err
	}

	timestamp := int64(binary.BigEndian.Uint64(header[0:8]))
	length := binary.BigEndian.Uint32(header[8:12])
	checksum := binary.BigEndian.Uint32(header[12:16])

	data := make([]byte, length)
	_, err = io.ReadFull(reader, data)
	if err != nil {
		return nil, err
	}

	offset := int(blockSize + length)

	return &LogEntry{
		Offset:    offset,
		Timestamp: timestamp,
		Checksum:  checksum,
		Data:      data,
		Length:    length,
	}, nil
}

func (s *Segment) SeekOffset(offset int64) (*LogEntry, error) {
	_, err := s.fd.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(s.fd)
	return s.ReadEntry(reader)
}

func (s *Segment) Size() int {
	return len(s.offset)
}
