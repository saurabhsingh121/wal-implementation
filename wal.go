package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
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

// Close closes the WAL files and calls sync on the WAL
func (wal *WAL) Close() error {
	wal.cancel()
	err := wal.Sync()
	if err != nil {
		return err
	}
	return wal.currentSegment.Close()
}

// WriteEntry writes an entry to the WAL
func (wal *WAL) WriteEntry(data []byte) error {
	return wal.writeEntry(data, false)
}

func (wal *WAL) writeEntry(data []byte, isCheckpoint bool) error {
	wal.lock.Lock()
	defer wal.lock.Unlock()
	err := wal.rotateLogIfNeeded()
	if err != nil {
		return err
	}

	wal.lastSequenceNumber++
	entry := &WAL_Entry{
		LogSequenceNumber: wal.lastSequenceNumber,
		Data:              data,
		CRC:               crc32.ChecksumIEEE(append(data, byte(wal.lastSequenceNumber))),
	}

	if isCheckpoint {
		err = wal.Sync()
		if err != nil {
			return err
		}
		entry.IsCheckpoint = &isCheckpoint
	}

	return wal.writeEntryToBuffer(entry)
}

func (wal *WAL) rotateLogIfNeeded() error {
	fileInfo, err := wal.currentSegment.Stat()
	if err != nil {
		return err
	}

	if fileInfo.Size()+int64(wal.bufWriter.Buffered()) >= wal.maxFileSize {
		err := wal.rotateLog()
		if err != nil {
			return err
		}
	}
	return nil
}

func (wal *WAL) rotateLog() error {
	err := wal.Sync()
	if err != nil {
		return err
	}

	err = wal.currentSegment.Close()
	if err != nil {
		return err
	}

	wal.currentSegmentIndex++
	if wal.currentSegmentIndex >= wal.maxSegments {
		err := wal.deleteOldestSegment()
		if err != nil {
			return nil
		}
	}

	newFile, err := createSegmentFile(wal.directory, wal.currentSegmentIndex)
	if err != nil {
		return err
	}

	wal.currentSegment = newFile
	wal.bufWriter = bufio.NewWriter(newFile)

	return nil
}

// deleteOldestSegment removes the oldest log file
func (wal *WAL) deleteOldestSegment() error {
	files, err := filepath.Glob(filepath.Join(wal.directory, segmentPrefix+"*"))
	if err != nil {
		return err
	}

	var oldestSegmentFilePath string
	if len(files) > 0 {
		// find the oldest segment ID
		oldestSegmentFilePath, err = wal.findOldestSegmentFile(files)
		if err != nil {
			return err
		}
	} else {
		return nil
	}

	// delete the oldest segment file
	err = os.Remove(oldestSegmentFilePath)
	if err != nil {
		return err
	}

	return nil
}

func (wal *WAL) findOldestSegmentFile(files []string) (string, error) {
	var oldestSegmentFilePath string
	oldestSegmentID := math.MaxInt64
	for _, file := range files {
		// get the segment index from the filename
		segmentIndex, err := strconv.Atoi(strings.TrimPrefix(file, filepath.Join(wal.directory, segmentPrefix)))

		if err != nil {
			return "", err
		}
		if segmentIndex < oldestSegmentID {
			oldestSegmentID = segmentIndex
			oldestSegmentFilePath = file
		}
	}

	return oldestSegmentFilePath, nil
}

func (wal *WAL) writeEntryToBuffer(entry *WAL_Entry) error {
	marshaledEntry := MustMarshal(entry)

	size := int32(len(marshaledEntry))
	err := binary.Write(wal.bufWriter, binary.LittleEndian, size)
	if err != nil {
		return err
	}
	_, err = wal.bufWriter.Write(marshaledEntry)
	if err != nil {
		return err
	}
	return nil
}

// ReadAll reads all the entries from the WAL. if readFromCheckpoint is true, it will return
// all the entries from the last checkpoint. If no entries, found it will return an empty slice
func (wal *WAL) ReadAll(readFromCheckpoint bool) ([]*WAL_Entry, error) {
	file, err := os.OpenFile(wal.currentSegment.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	entries, checkpoint, err := readAllEntriesFromFile(file, readFromCheckpoint)
	if err != nil {
		return nil, err
	}

	if readFromCheckpoint && checkpoint <= 0 {
		return entries[:0], nil
	}
	return entries, nil
}

func readAllEntriesFromFile(file *os.File, readFromCheckpoint bool) ([]*WAL_Entry, uint64, error) {
	var entries []*WAL_Entry
	checkpointLogSequenceNo := uint64(0)
	for {
		var size int32
		err := binary.Read(file, binary.LittleEndian, &size)
		if err != nil {
			if err == io.EOF {
				break
			}
			return entries, checkpointLogSequenceNo, err
		}

		data := make([]byte, size)
		_, err = io.ReadFull(file, data)
		if err != nil {
			return entries, checkpointLogSequenceNo, err
		}

		entry, err := unmarshalAndVerifyEntry(data)
		if err != nil {
			return entries, checkpointLogSequenceNo, err
		}

		// if we are reading from checkpoint and we find a checkpoint entry, we should return the entries from the last checkpoint. For that we are emptying the entries slice and start appending from that checkpoint
		if entry.IsCheckpoint != nil && entry.GetIsCheckpoint() {
			checkpointLogSequenceNo = entry.GetLogSequenceNumber()
			// emptying the entries slice
			entries = entries[:0]
		}
		entries = append(entries, entry)

	}
	return entries, checkpointLogSequenceNo, nil
}

// Repair repairs a corrupted WAL by scanning the WAL from the start and reading all entries until a
// corrupted entry is encountered, at which point the file is truncated. The function returns the
// entries that were read before the corruption and overwrites the existing WAL file with the
// repaired entries. It checks the CRC of each entry to verify if it is corrupted, and if the CRC is
// invalid, the file is truncated at that point.
func (wal *WAL) Repair() ([]*WAL_Entry, error) {
	files, err := filepath.Glob(filepath.Join(wal.directory, segmentPrefix+"*"))
	if err != nil {
		return nil, err
	}
	var lastSegmentID int
	if len(files) > 0 {
		// find the last segment ID
		lastSegmentID, err = findLastSegmentIndexInFiles(files)
		if err != nil {
			return nil, err
		}
	} else {
		log.Fatalf("No log segments found. Nothing to repair")
	}

	// open the last log segment file
	filePath := filepath.Join(wal.directory, fmt.Sprintf("%s%d", segmentPrefix, lastSegmentID))
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	// seek to the begining of the file
	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	var entries []*WAL_Entry

	for {
		// read the size of the next entry
		var size int32
		err = binary.Read(file, binary.LittleEndian, &size)
		if err != nil {
			if err == io.EOF {
				// end of the line reached, no corruption found
				return entries, err
			}
			log.Printf("error while reading entry size: %v", err)
			err = wal.replaceWithFixedFile(entries)
			if err != nil {
				return entries, err
			}
			return nil, nil
		}

		// read the entry data
		data := make([]byte, size)
		_, err = io.ReadFull(file, data)
		if err != nil {
			// truncate the file at this point
			err = wal.replaceWithFixedFile(entries)
			if err != nil {
				return entries, err
			}
			return entries, nil
		}

		// deserialize the entry
		var entry WAL_Entry
		err = proto.Unmarshal(data, &entry)
		if err != nil {
			if err := wal.replaceWithFixedFile(entries); err != nil {
				return entries, err
			}
			return entries, nil
		}

		if !verifyCRC(&entry) {
			log.Printf("CRC mismatch: data may be corrupted")
			// truncate the file at this point
			if err := wal.replaceWithFixedFile(entries); err != nil {
				return entries, err
			}
			return entries, nil
		}
		// Add the entry to the slice.
		entries = append(entries, &entry)
	}
}

// replaceWithFixedFile replaces the existing WAL file with the given entries atomically.
func (wal *WAL) replaceWithFixedFile(entries []*WAL_Entry) error {
	// create a temp file to make operation atomic
	tempFilePath := fmt.Sprintf("%s.tmp", wal.currentSegment.Name())
	tempFile, err := os.OpenFile(tempFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	// write the entries to the temporary file
	for _, entry := range entries {
		marshaledEntry := MustMarshal(entry)

		size := int32(len(marshaledEntry))
		err = binary.Write(tempFile, binary.LittleEndian, size)
		if err != nil {
			return err
		}

		_, err := tempFile.Write(marshaledEntry)
		if err != nil {
			return err
		}
	}

	// close the temporary file
	err = tempFile.Close()
	if err != nil {
		return err
	}

	// rename the temporary file to the original file name
	err = os.Rename(tempFilePath, wal.currentSegment.Name())
	if err != nil {
		return err
	}

	return nil
}
