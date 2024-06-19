package wal

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testLogEntryMessage = "log entry"
)

func runLayoutTest(t *testing.T, maxOffset int, test func(wal *WAL)) {
	w, err := New(
		WithDir("./tests/logs"),
		WithPrefix("examples-wal"),
		WithMaxSegmentSize(5*1024*1024), //  5MB (Log rotation size)
		WithMaxSegmentFile(5),           // maximum number of segment files
	)
	defer os.RemoveAll("./tests/logs")
	if err != nil {
		panic(err)
	}

	for i := 0; i <= maxOffset; i++ {
		data := fmt.Sprintf("%s %d", testLogEntryMessage, i)
		if err := w.Write([]byte(data)); err != nil {
			fmt.Printf("Error writing to WAL: %v\n", err)
			return
		}
	}

	test(w)

	t.Cleanup(func() {
		w.Close()
	})
}

func TestWrite(t *testing.T) {
	t.Run("Success Write to Logs", func(t *testing.T) {
		runLayoutTest(t, 10, func(wal *WAL) {
			entry, err := wal.ReadIndex(0)
			require.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("%s %d", testLogEntryMessage, 0), string(entry.Data))
		})
	})

	t.Run("Success Write to Logs and Read From Buffer", func(t *testing.T) {
		runLayoutTest(t, 500000, func(wal *WAL) {
			entry, err := wal.ReadIndex(wal.CurrentPosition())
			require.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("%s %d", testLogEntryMessage, 500000), string(entry.Data))
		})
	})

	t.Run("Write - Sync - Write", func(t *testing.T) {
		runLayoutTest(t, 50, func(wal *WAL) {
			entry, err := wal.ReadIndex(wal.CurrentPosition())
			require.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("%s 50", testLogEntryMessage), string(entry.Data))

			err = wal.Sync()
			require.NoError(t, err)

			for i := 51; i <= 100; i++ {
				wal.Write([]byte(fmt.Sprintf("%s %d", testLogEntryMessage, i)))
			}

			entry, err = wal.ReadIndex(wal.CurrentPosition())
			require.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("%s 100", testLogEntryMessage), string(entry.Data))
		})
	})
}

func TestIter(t *testing.T) {
	t.Run("Success Iteration on File", func(t *testing.T) {
		runLayoutTest(t, 200000, func(wal *WAL) {
			expecteds := make([]string, 0)
			for i := 0; i <= 50; i++ {
				expecteds = append(expecteds, fmt.Sprintf("%s %d", testLogEntryMessage, i))
			}

			actuals := make([]string, 0)
			idx := 0
			wal.Iter(func(index int, entry *LogEntry) bool {
				actuals = append(actuals, string(entry.Data))
				if idx >= 50 {
					return false
				}
				idx++
				return true
			})

			assert.Equal(t, expecteds, actuals)
		})
	})

	t.Run("Success Iteration on Buffer", func(t *testing.T) {
		runLayoutTest(t, 100, func(wal *WAL) {
			expecteds := make([]string, 0)
			for i := 0; i <= 50; i++ {
				expecteds = append(expecteds, fmt.Sprintf("%s %d", testLogEntryMessage, i))
			}

			actuals := make([]string, 0)
			idx := 0
			wal.Iter(func(index int, entry *LogEntry) bool {
				actuals = append(actuals, string(entry.Data))
				if idx >= 50 {
					return false
				}
				idx++
				return true
			})

			assert.Equal(t, expecteds, actuals)
		})
	})

	t.Run("Success Iteration on Buffer & File", func(t *testing.T) {
		runLayoutTest(t, 500000, func(wal *WAL) {
			err := wal.Iter(func(index int, entry *LogEntry) bool {
				return true
			})
			require.NoError(t, err)
		})
	})

	t.Run("Success Iteration Reverse on Buffer", func(t *testing.T) {
		runLayoutTest(t, 100, func(wal *WAL) {
			expecteds := make([]string, 0)
			for i := 100; i >= 0; i-- {
				expecteds = append(expecteds, fmt.Sprintf("%s %d", testLogEntryMessage, i))
			}

			actuals := make([]string, 0)
			wal.IterReverse(func(index int, entry *LogEntry) bool {
				actuals = append(actuals, string(entry.Data))
				return true
			})
			assert.Equal(t, expecteds, actuals)
		})
	})
}
