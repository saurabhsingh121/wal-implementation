package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	syncInterval  = 200 * time.Millisecond
	segmentPrefix = "segment-"
)

// WAL structure
type WAL struct {
	directory           string
	currentSegment      *os.File
	lock                sync.Mutex
	lastSequenceNumber  uint64
	bufWriter           *bufio.Writer
	syncTimer           *time.Timer
	shouldFsync         bool
	maxFileSize         int64
	maxSegments         int
	currentSegmentIndex int
	ctx                 context.Context
	cancel              context.CancelFunc
}

func OpenWal(directory string, enableFsync bool, maxFileSize int64, maxSegments int) (*WAL, error) {
	// create a directory if doesn't exist already
	err := os.MkdirAll(directory, 0755)
	if err != nil {
		return nil, err
	}
	// get the list of all segment file in the directory
	files, err := filepath.Glob(filepath.Join(directory, segmentPrefix+"*"))
	if err != nil {
		return nil, err
	}
	var lastSegmentID int
	if len(files) > 0 {
		// find the last segment id
		lastSegmentID, err = findLastSegmentIndexInFiles(files)
		if err != nil {
			return nil, err
		}
	} else {
		// create the first log segment
		file, err := createSegmentFile(directory, 0)
		if err != nil {
			return nil, err
		}
		err = file.Close()
		if err != nil {
			return nil, err
		}
	}
	// open the last log segment file
	filePath := filepath.Join(directory, fmt.Sprintf("%s%d", segmentPrefix, lastSegmentID))
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	// go the end of the file
	_, err = file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	wal := &WAL{
		directory:           directory,
		currentSegment:      file,
		lastSequenceNumber:  0,
		bufWriter:           bufio.NewWriter(file),
		syncTimer:           time.NewTimer(syncInterval),
		shouldFsync:         enableFsync,
		maxFileSize:         maxFileSize,
		maxSegments:         maxSegments,
		currentSegmentIndex: lastSegmentID,
		ctx:                 ctx,
		cancel:              cancel,
	}

	lastSequenceNumber, err := wal.getLastSequenceNo()
	if err != nil {
		return nil, err
	}
	wal.lastSequenceNumber = lastSequenceNumber

	go wal.keepSyncing()

	return wal, nil
}

// getLastSequenceNo returns the last sequence number in the log
func (wal *WAL) getLastSequenceNo() (uint64, error) {
	entry, err := wal.getLastEntryInLog()
	if err != nil {
		return 0, err
	}

	if entry != nil {
		return entry.GetLogSequenceNumber(), nil
	}
	return 0, nil
}

// getLastEntryInLog goes the last entry of the segment file and returns the last entry
func (wal *WAL) getLastEntryInLog() (*WAL_Entry, error) {
	file, err := os.OpenFile(wal.currentSegment.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var previousSize int32
	var offset int64
	var entry *WAL_Entry
	for {
		var size int32
		err = binary.Read(file, binary.LittleEndian, &size)
		if err != nil {
			if err == io.EOF {
				// end of the line reached, read the last entry at the saved offset
				if offset == 0 {
					return entry, nil
				}

				_, err = file.Seek(offset, io.SeekStart)
				if err != nil {
					return nil, err
				}

				// read the entry data
				data := make([]byte, previousSize)
				_, err = io.ReadFull(file, data)
				if err != nil {
					return nil, err
				}

				entry, err := unmarshalAndVerifyEntry(data)
				if err != nil {
					return nil, err
				}
				return entry, nil
			}
			return nil, err
		}
		// get current offset
		previousSize = size
		offset, err = file.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}

		// skip to the next entry
		_, err = file.Seek(int64(size), io.SeekCurrent)
		if err != nil {
			return nil, err
		}
	}
}

func (wal *WAL) keepSyncing() {
	for {
		select {
		case <-wal.syncTimer.C:
			wal.lock.Lock()
			err := wal.Sync()
			wal.lock.Unlock()
			if err != nil {
				log.Printf("Error while performing sync: %v", err)
			}
		case <-wal.ctx.Done():
			return
		}
	}
}

// Sync writes out any data in WAL's in memory buffer to segment file. If fsync is enabled,
// it also calls fsync on the segment file. It also resets the syncronization timer.
func (wal *WAL) Sync() error {
	err := wal.bufWriter.Flush()
	if err != nil {
		return err
	}
	if wal.shouldFsync {
		err = wal.currentSegment.Sync()
		if err != nil {
			return err
		}
	}
	// reset the keepSyncing timer, because we just synced
	wal.resetTimer()
	return nil
}

// resetTimer resets the syncronization timer.
func (wal *WAL) resetTimer() {
	wal.syncTimer.Reset(syncInterval)
}
