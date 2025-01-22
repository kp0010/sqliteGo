package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

func readPageSize(dbFile os.File) (pageSize uint16) {
	// Reading Header to Find Page Size
	header := make([]byte, 100)

	_, err := dbFile.Read(header)
	if err != nil {
		log.Fatal(err)
	}

	if err := binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &pageSize); err != nil {
		fmt.Println("Failed to read integer:", err)
		return
	}

	return
}

func readCellCount(dbFile os.File, pageSize uint16) (cellCount uint16) {
	// Reading Cells in Page that Indicate Table numbers
	pageHeader := make([]byte, pageSize)

	_, err := dbFile.ReadAt(pageHeader, 100)
	if err != nil {
		log.Fatal(err)
	}

	if err := binary.Read(bytes.NewReader(pageHeader[3:5]), binary.BigEndian, &cellCount); err != nil {
		fmt.Println("Failed to read Cell Count", err)
		return
	}

	return
}

func readCellPointers(dbFile os.File, cellCount uint16) []uint16 {
	// Reading after Page Header and Finding all Cell offsets
	cellPointersBytes := make([]byte, cellCount*2)

	_, err := dbFile.ReadAt(cellPointersBytes, 100+8)

	if err != nil {
		log.Fatal(err)
	}

	cellPointers := make([]uint16, cellCount)

	for i := 0; i < int(cellCount); i++ {
		if err := binary.Read(bytes.NewReader(cellPointersBytes[i*2:(i*2)+2]), binary.BigEndian, &cellPointers[i]); err != nil {
			fmt.Println("Failed to read Cell Pointers", err)
			return cellPointers
		}
	}

	return cellPointers
}

func readRecords(dbFile os.File, cellPointers []uint16) [][]byte {
	records := make([][]byte, len(cellPointers))

	for i, pointer := range cellPointers {
		recordBytes := make([]byte, 1)

		_, err := dbFile.ReadAt(recordBytes, int64(pointer))
		if err != nil {
			log.Fatal(err)
		}

		var recordSize uint8 = uint8(recordBytes[0])

		recordBytes = make([]byte, recordSize)
		_, err = dbFile.ReadAt(recordBytes, int64(pointer))
		if err != nil {
			log.Fatal(err)
		}

		records[i] = recordBytes
	}

	return records
}

func parseRecords(records [][]byte) {
	for _, record := range records {
		var rowId int8 = int8(record[1])
		var recordHeaderSize int8 = int8(record[2])
		var sqliteSizeMap = map[string][]byte{
			"type":       nil,
			"name":       nil,
			"table_name": nil,
			"rootpage":   nil,
			"sql":        nil,
		}

		i := 3
		for key := range sqliteSizeMap {
			sqliteSizeMap[key] = append(sqliteSizeMap[key], record[i])
			for record[i]&128 != 0 {
				i++
				sqliteSizeMap[key] = append(sqliteSizeMap[key], record[i])
			}
			i++
		}

		fmt.Println(rowId, recordHeaderSize, sqliteSizeMap)
	}
}

// Usage: your_program.sh sample.db .dbinfo | .tables
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
		dbFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		pageSize := readPageSize(*dbFile)
		cellCount := readCellCount(*dbFile, pageSize)
		cellPointers := readCellPointers(*dbFile, cellCount)

		fmt.Printf("database page size: %v\n", pageSize)
		fmt.Printf("number of tables: %v\n", cellCount)
		fmt.Printf("cell pointers: %v\n", cellPointers)

	case ".tables":
		dbFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		pageSize := readPageSize(*dbFile)
		cellCount := readCellCount(*dbFile, pageSize)
		cellPointers := readCellPointers(*dbFile, cellCount)
		records := readRecords(*dbFile, cellPointers)
		parseRecords(records)

		// fmt.Printf(string(records[0]))

	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
