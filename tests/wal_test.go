package tests

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/saurabhsingh121/wal-implementation"
	"github.com/stretchr/testify/assert"
)

const (
	maxSegments = 3
	maxFileSize = 64 * 1000 * 1000 // 64MB
)

func TestWAL_WriteAndRecover(t *testing.T) {
	t.Parallel()
	// setup -> create a temporary file for the WAL
	dirPath := "TestWAL_WriteAndRecover.log"
	defer os.RemoveAll(dirPath) // cleanup after the test
	walog, err := wal.OpenWal(dirPath, true, maxFileSize, maxSegments)
	assert.NoError(t, err, "Failed to create WAL")
	defer walog.Close()

	// test data
	entries := []Record{
		{Key: "key1", Value: []byte("value1"), Op: InsertOperation},
		{Key: "key2", Value: []byte("value2"), Op: InsertOperation},
		{Key: "key3", Op: DeleteOperation},
	}

	// write entries to the WAL
	for _, entry := range entries {
		marshedEntry, err := json.Marshal(entry)
		assert.NoError(t, err, "Failed to marshal entry")
		assert.NoError(t, walog.WriteEntry(marshedEntry), "Failed to write entry")
	}

	// recover entries from WAL
	recoveredEntries, err := walog.ReadAll(false)
	assert.NoError(t, err, "Failed to recover entries")

	// check if recovered entries match the written entries
	for entryIndex, entry := range recoveredEntries {
		unmarshaledEntry := Record{}
		assert.NoError(t, json.Unmarshal(entry.Data, &unmarshaledEntry), "Failed to unmarshal entry")

		// can't use deep equal because of the sequence number
		assert.Equal(t, entries[entryIndex].Key, unmarshaledEntry.Key, "Recovered entry does not match written entry (Key)")
		assert.Equal(t, entries[entryIndex].Op, unmarshaledEntry.Op, "Recovered entry does not match written entry (Op)")
		assert.Equal(t, reflect.DeepEqual(entries[entryIndex].Value, unmarshaledEntry.Value), "Recovered entry does not match written entry (Value)")
	}
}

// test to verify that log sequence number are incremented correctly after reopening the WAL
func TestWAL_LogSequenceNumber(t *testing.T) {
	t.Parallel()
	dirPath := "TestWAL_LogSequenceNumber.log"
	defer os.RemoveAll(dirPath) // Cleanup after the test

	walog, err := wal.OpenWal(dirPath, true, maxFileSize, maxSegments)
	assert.NoError(t, err, "Failed to create WAL")

	// test data
	entries := []Record{
		{Key: "key1", Value: []byte("value1"), Op: InsertOperation},
		{Key: "key2", Value: []byte("value2"), Op: InsertOperation},
		{Key: "key3", Op: DeleteOperation},
		{Key: "key4", Value: []byte("value1"), Op: InsertOperation},
		{Key: "key5", Value: []byte("value2"), Op: InsertOperation},
		{Key: "key6", Op: DeleteOperation},
	}

	// write entries to the WAL
	for i := 0; i < 3; i++ {
		entry := entries[i]
		marshaledEntry, err := json.Marshal(entry)
		assert.NoError(t, err, "Failed to marshal entry")
		assert.NoError(t, walog.WriteEntry(marshaledEntry), "Failed to write entry")
	}

	// close the WAL
	assert.NoError(t, walog.Close(), "Failed to close WAL")

	// reopen the WAL
	walog, err = wal.OpenWal(dirPath, true, maxFileSize, maxSegments)
	assert.NoError(t, err, "Failed to reopen WAL")

	// Write entries to WAL
	for i := 3; i < 6; i++ {
		entry := entries[i]
		marshaledEntry, err := json.Marshal(entry)
		assert.NoError(t, err, "Failed to marshal entry")
		assert.NoError(t, walog.WriteEntry(marshaledEntry), "Failed to write entry")
	}

	// important to check if entries are flushed to the disk
	assert.NoError(t, walog.Close(), "Failed to close WAL")

	// recover entries from the WAL
	recoveredEntries, err := walog.ReadAll(false)
	assert.NoError(t, err, "Failed to recover entries")
	assertCollectionsAreIdentical(t, entries, recoveredEntries)
}

func assertCollectionsAreIdentical(t *testing.T, expected []Record, actual []*wal.WAL_Entry) {
	assert.Equal(t, len(expected), len(actual), "Number of entries do not match")

	for entryIndex, entry := range actual {
		unmarshaledEntry := Record{}
		assert.NoError(t, json.Unmarshal(entry.Data, &unmarshaledEntry), "Failed to unmarshal entry")

		// can't use deep equal because of the sequence number
		assert.Equal(t, expected[entryIndex].Key, unmarshaledEntry.Key, "Recovered entry does not match written entry (Key)")
		assert.Equal(t, expected[entryIndex].Op, unmarshaledEntry.Op, "Recovered entry does not match written entry (Op)")
		assert.True(t, reflect.DeepEqual(expected[entryIndex].Value, unmarshaledEntry.Value), "Recovered entry does not match written entry (Value)")
	}
}

func TestWAL_WriteRepairRead(t *testing.T) {
	t.Parallel()
	dirPath := "TestWAL_WriteRepairRead"
	defer os.RemoveAll(dirPath)

	// create a new WAL
	walog, err := wal.OpenWal(dirPath, true, maxFileSize, maxSegments)
	assert.NoError(t, err)

	// write some entries to the WAL
	err = walog.WriteEntry([]byte("entry1"))
	assert.NoError(t, err)
	err = walog.WriteEntry([]byte("entry2"))
	assert.NoError(t, err)

	walog.Close()

	// corrupt the WAL by writing some random data
	file, err := os.OpenFile(filepath.Join(dirPath, "segment-0"), os.O_APPEND|os.O_WRONLY, 0644)
	assert.NoError(t, err)

	_, err = file.Write([]byte("random data"))
	assert.NoError(t, err)
	file.Close()

	// repair the WAL
	entries, err := walog.Repair()
	assert.NoError(t, err)

	// check that the current entries were recovered
	assert.Equal(t, 2, len(entries))
	assert.Equal(t, "entry1", string(entries[0].Data))
	assert.Equal(t, "entry2", string(entries[1].Data))

	// check that the WAL is usable
	walog, err = wal.OpenWal(dirPath, true, maxFileSize, maxSegments)
	assert.NoError(t, err)

	err = walog.WriteEntry([]byte("entry3"))
	assert.NoError(t, err)

	walog.Close()

	// check that the correct entries were recovered
	entries, err = walog.ReadAll(false)
	assert.NoError(t, err)

	assert.Equal(t, 3, len(entries))
	assert.Equal(t, "entry1", string(entries[0].Data))
	assert.Equal(t, "entry2", string(entries[1].Data))
	assert.Equal(t, "entry3", string(entries[2].Data))
}
