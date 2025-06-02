package tests

import (
	"encoding/json"
	"os"
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
