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

		recordBytes = make([]byte, recordSize+2)
		_, err = dbFile.ReadAt(recordBytes, int64(pointer))
		if err != nil {
			log.Fatal(err)
		}

		records[i] = recordBytes
	}

	return records
}

func convSizeToSerialType(contentSize uint16) (serialType uint16) {
	switch {
	case (contentSize >= 0 && contentSize <= 4):
		return uint16(contentSize)
	case (contentSize == 5):
		return uint16(6)
	case (contentSize == 6) || (contentSize == 7):
		return uint16(8)
	case (contentSize == 8) || (contentSize == 9):
		return uint16(1 << (24 - contentSize))
	case (contentSize >= 12) && (contentSize&1 != 1):
		return uint16((contentSize - 12) / 2)
	case (contentSize >= 13) && (contentSize&1 == 1):
		return uint16((contentSize - 13) / 2)
	}

	return uint16(0)
}

func parseRecords(records [][]byte, cellPointers []uint16) []map[string]uint16 {
	var sqliteSizeMaps []map[string]uint16

	for idx, record := range records {
		var rowId uint16 = uint16(record[1])
		var recordHeaderSize int8 = int8(record[2])

		var sqliteSizeIndices = []string{"type", "name", "table_name", "rootpage", "sql"}

		var sqliteSizeMap = map[string]uint16{
			"rowId":            rowId,
			"cell_pointer":     cellPointers[idx],
			"recordHeaderSize": uint16(recordHeaderSize),
		}

		i := 3
		for _, key := range sqliteSizeIndices {
			msbRem := int(record[i]) & 0b1111111
			sqliteSizeMap[key] = sqliteSizeMap[key]*128 + uint16(msbRem)
			for record[i]&128 != 0 {
				i++
				msbRem := int(record[i]) & 0b1111111
				sqliteSizeMap[key] = sqliteSizeMap[key]*128 + uint16(msbRem)
			}
			i++
			sqliteSizeMap[key] = convSizeToSerialType(sqliteSizeMap[key])
		}

		sqliteSizeMaps = append(sqliteSizeMaps, sqliteSizeMap)
	}

	return sqliteSizeMaps
}

func parseRecordsSizeMaps(recordSizeMaps []map[string]uint16, records [][]byte) []map[string]string {
	var sqliteSizeIndices = []string{"type", "name", "table_name", "rootpage", "sql"}
	recordNames := make([]map[string]string, len(recordSizeMaps))
	var offset uint16 = 2

	for idx, recordSizeMap := range recordSizeMaps {
		offset = 2 + recordSizeMap["recordHeaderSize"]
		recordNames[idx] = make(map[string]string)

		for _, index := range sqliteSizeIndices {
			var res string
			if index == "rootpage" {
				res = fmt.Sprintf("%x", records[idx][offset:offset+recordSizeMap[index]])
				offset += 1
			} else {
				res = string(records[idx][offset : offset+recordSizeMap[index]])
				offset += recordSizeMap[index]
			}
			recordNames[idx][index] = res
		}
	}

	return recordNames
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

		fmt.Printf("database page size: %v\n", pageSize)
		fmt.Printf("number of tables: %v\n", cellCount)

	case ".tables":
		dbFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		pageSize := readPageSize(*dbFile)
		cellCount := readCellCount(*dbFile, pageSize)
		cellPointers := readCellPointers(*dbFile, cellCount)

		var records [][]byte = readRecords(*dbFile, cellPointers)
		var recordSizeMaps []map[string]uint16 = parseRecords(records, cellPointers)

		var recordNameMaps []map[string]string = parseRecordsSizeMaps(recordSizeMaps, records)

		for _, mp := range recordNameMaps {
			fmt.Print(mp["table_name"] + " ")
			// for key, val := range mp {
			// 	fmt.Println(key, ":", val)
			// }
		}
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
