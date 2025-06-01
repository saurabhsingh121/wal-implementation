package wal

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)


const (
	syncInterval = 200 * time.Millisecond
	segmentPrefix = "segment-"
)

// WAL structure
type WAL struct {
	directory string
	currentSegment *os.File
	lock sync.Mutex
	lastSequenceNumber uint64
	bufWriter *bufio.Writer
	syncTimer *time.Timer
	shouldFsync bool
	maxFileSize int64
	maxSegments int
	currentSegmentIndex int
	ctx context.Context
	cancel context.CancelFunc
}

func OpenWal(directory string, enableFsync bool, maxFileSize int64, maxSegments int) (*WAL, error){
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
	}else {
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
	file, err := os.OpenFile(filePath, os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0644)
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
		directory: directory,
		currentSegment: file,
		lastSequenceNumber: 0,
		bufWriter: bufio.NewWriter(file),
		syncTimer: time.NewTimer(syncInterval),
		shouldFsync: enableFsync,
		maxFileSize: maxFileSize,
		maxSegments: maxSegments,
		currentSegmentIndex: lastSegmentID,
		ctx: ctx,
		cancel: cancel,
	}
	
	lastSequenceNumber, err := wal.getLastSequenceNo()
	if err != nil {
		return nil, err
	}
	wal.lastSequenceNumber = lastSequenceNumber
	return wal, nil
}

func (wal *WAL) getLastSequenceNo() (uint64, error){
	return 0, nil
}