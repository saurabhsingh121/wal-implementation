package wal

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

func MustMarshal(entry *WAL_Entry) []byte{
	marshaledEntry, err := proto.Marshal(entry)
	if err != nil {
		panic(fmt.Sprintf("marshal should never fail (%v)", err))
	}
	return marshaledEntry
}

func MustUnmarshal(data []byte, entry *WAL_Entry) {
	if err := proto.Unmarshal(data, entry); err != nil {
		panic(fmt.Sprintf("unmarshal should never fail (%v)", err))
	}
}