package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func findLastSegmentIndexInFiles(files []string) (int, error){
	var lastSegmentID int
	for _, file := range files {
		_, fileName := filepath.Split(file)
		segmentID, err := strconv.Atoi(strings.TrimPrefix(fileName, segmentPrefix))
		if err != nil {
			return 0, err
		}
		if segmentID > lastSegmentID {
			lastSegmentID = segmentID
		}
	}
	return lastSegmentID, nil
}

func createSegmentFile(directory string, segmentID int) (*os.File, error){
	filePath := filepath.Join(directory, fmt.Sprintf("%s%d", segmentPrefix, segmentID))
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	return file, nil
}