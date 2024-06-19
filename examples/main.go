package main

import (
	"fmt"

	"github.com/yudhasubki/wal"
)

func main() {
	w, err := wal.New(
		wal.WithDir("./logs"),
		wal.WithPrefix("examples-wal"),
		wal.WithMaxSegmentSize(5*1024*1024), //  5MB (Log rotation size)
		wal.WithMaxSegmentFile(5),           // maximum number of segment files
		wal.WithMaxFileLifetime(3),
		wal.WithCustomJanitorHook(wal.DefaultJanitorHook),
	)
	if err != nil {
		panic(err)
	}
	defer w.Close()

	for i := 0; i < 200000; i++ {
		data := fmt.Sprintf("log entry %d\n", i)
		if err := w.Write([]byte(data)); err != nil {
			fmt.Printf("Error writing to WAL: %v\n", err)
			return
		}
	}

	idx := 0
	err = w.Iter(func(index int, entry *wal.LogEntry) bool {
		if idx == 10 {
			return false
		}
		fmt.Println(index, " ", entry)
		idx++
		return true
	})
	if err != nil {
		panic(err)
	}

	e, err := w.ReadIndex(1)
	if err != nil {
		panic(err)
	}
	fmt.Println("Start Entry Position ", string(e.Data))

	e, err = w.ReadIndex(w.CurrentPosition() / 2)
	if err != nil {
		panic(err)
	}
	fmt.Println("Mid Entry Position ", string(e.Data))

	e, err = w.ReadIndex(w.CurrentPosition())
	if err != nil {
		panic(err)
	}
	fmt.Println("Last Entry Position ", string(e.Data))

	fmt.Println("WAL logging complete")

}
