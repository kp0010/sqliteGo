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

func parseVarint(inputBytes []byte) (uint64, int) {
	var res uint64 = 0
	var i int = 0

	msbRem := int(inputBytes[i]) & 0b1111111
	res = res*128 + uint64(msbRem)

	for inputBytes[i]&128 != 0 {
		i++
		msbRem := int(inputBytes[i]) & 0b1111111
		res = res*128 + uint64(msbRem)
	}

	return res, i + 1
}

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

func readCellCount(dbFile os.File, pageSize uint16, pageIndex ...uint16) (cellCount uint16) {
	// Reading Cells in Page that Indicate Table numbers
	pageHeader := make([]byte, pageSize)

	var offset uint16 = 100
	if len(pageIndex) > 0 {
		offset += (pageIndex[0] - 1) * pageSize
		if pageIndex[0] != 1 {
			offset -= 100
		}
	}

	// fmt.Println(offset)
	_, err := dbFile.ReadAt(pageHeader, int64(offset))
	if err != nil {
		log.Fatal(err)
	}

	if err := binary.Read(bytes.NewReader(pageHeader[3:5]), binary.BigEndian, &cellCount); err != nil {
		fmt.Println("Failed to read Cell Count", err)
		return
	}

	return
}

func readCellPointers(dbFile os.File, cellCount uint16, pageSize uint16, pageIndex ...uint16) []uint16 {
	// Reading after Page Header and Finding all Cell offsets
	cellPointersBytes := make([]byte, cellCount*2)

	var offset uint16 = 100
	if len(pageIndex) > 0 {
		offset += (pageIndex[0] - 1) * pageSize
		if pageIndex[0] != 1 {
			offset -= 100
		}
	}

	// fmt.Println(offset)
	_, err := dbFile.ReadAt(cellPointersBytes, int64(offset+8))

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

func readPageHeader(dbFile os.File, pageSize uint16, pageIndex ...uint16) map[string]uint16 {
	var pageHeaderIndices = []string{
		"page_type",
		"freeblock_start",
		"cell_count",
		"cell_content_start",
		"frag_count",
		"page_number",
	}

	var res = make(map[string]uint16, 6)

	pageHeaderBytes := make([]byte, 12)

	var offset uint16 = 100
	if len(pageIndex) > 0 {
		offset += (pageIndex[0] - 1) * pageSize
		if pageIndex[0] != 1 {
			offset -= 100
		}
	}

	_, err := dbFile.ReadAt(pageHeaderBytes, int64(offset))
	if err != nil {
		log.Fatal(err)
	}

	i := 0
	for _, index := range pageHeaderIndices {
		if index == "page_type" || index == "frag_count" {
			res[index] = uint16(pageHeaderBytes[i])
			i++
		} else if (res["page_type"] <= 0x05) && (index == "page_number") {
			binary.Read(bytes.NewReader(pageHeaderBytes[i:i+4]), binary.BigEndian, res[index])
			i += 4
		} else {
			var temp uint16
			binary.Read(bytes.NewReader(pageHeaderBytes[i:i+2]), binary.BigEndian, &temp)
			res[index] = temp
			i += 2
		}
	}

	return res
}

func readRecords(dbFile os.File, cellPointers []uint16, pageSize uint16, pageIndex ...uint16) ([][]byte, []int) {
	records := make([][]byte, len(cellPointers))
	varIntSizes := make([]int, len(cellPointers))

	for i, pointer := range cellPointers {
		recordBytes := make([]byte, 8)

		var offset uint16 = 0
		if len(pageIndex) > 0 {
			offset += (pageIndex[0] - 1) * pageSize
		}

		_, err := dbFile.ReadAt(recordBytes, int64(pointer+offset))
		if err != nil {
			log.Fatal(err)
		}

		recordSize, size := parseVarint(recordBytes)

		recordBytes = make([]byte, int(recordSize)+1+size)
		_, err = dbFile.ReadAt(recordBytes, int64(pointer+offset))
		if err != nil {
			log.Fatal(err)
		}

		records[i] = recordBytes
		varIntSizes[i] = size
	}

	return records, varIntSizes
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

func parseRecords(records [][]byte, varIntSizes []int, cellPointers []uint16) []map[string]uint16 {
	var sqliteSizeMaps []map[string]uint16

	for idx, record := range records {
		varIntSize := varIntSizes[idx]
		var rowId uint16 = uint16(record[varIntSize])
		var recordHeaderSize int8 = int8(record[varIntSize+1])

		var sqliteSizeIndices = []string{"type", "name", "table_name", "rootpage", "sql"}

		var sqliteSizeMap = map[string]uint16{
			"rowId":            rowId,
			"cell_pointer":     cellPointers[idx],
			"recordHeaderSize": uint16(recordHeaderSize),
		}

		i := varIntSize + 2
		for _, key := range sqliteSizeIndices {
			// msbRem := int(record[i]) & 0b1111111
			// sqliteSizeMap[key] = sqliteSizeMap[key]*128 + uint16(msbRem)
			// for record[i]&128 != 0 {
			// 	i++
			// 	msbRem := int(record[i]) & 0b1111111
			// 	sqliteSizeMap[key] = sqliteSizeMap[key]*128 + uint16(msbRem)
			// }
			// i++
			// sqliteSizeMap[key] = convSizeToSerialType(sqliteSizeMap[key])

			varint, size := parseVarint(record[i:])
			i += size
			sqliteSizeMap[key] = convSizeToSerialType(uint16(varint))
		}

		sqliteSizeMaps = append(sqliteSizeMaps, sqliteSizeMap)
	}

	return sqliteSizeMaps
}

func parseRecordsSizeMaps(records [][]byte, varIntSizes []int, recordSizeMaps []map[string]uint16) []map[string]string {
	var sqliteSizeIndices = []string{"type", "name", "table_name", "rootpage", "sql"}
	recordNames := make([]map[string]string, len(recordSizeMaps))
	var offset uint16

	for idx, recordSizeMap := range recordSizeMaps {
		offset = uint16(varIntSizes[idx]) + recordSizeMap["recordHeaderSize"]
		recordNames[idx] = make(map[string]string)

		for _, index := range sqliteSizeIndices {
			var res string
			if index == "rootpage" {
				res = fmt.Sprintf("%x", records[idx][offset+1:offset+recordSizeMap[index]+1])
				offset += 1
			} else {
				res = string(records[idx][offset+1 : offset+recordSizeMap[index]+1])
				offset += recordSizeMap[index]
			}
			recordNames[idx][index] = res
		}
	}

	return recordNames
}

func readPage(dbFile os.File, pageSize uint16, pageIndex uint16) []byte {
	res := make([]byte, pageSize)
	// fmt.Println(pageSize)
	//
	// fmt.Println(int64(pageIndex-1) * int64(pageSize))

	dbFile.ReadAt(res, int64((pageIndex-1)*pageSize))

	// fmt.Println(res, len(res))
	return res
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

		var pageIndex uint16 = 1
		pageSize := readPageSize(*dbFile)

		var pageHeaders map[string]uint16 = readPageHeader(*dbFile, pageSize, pageIndex)
		var cellCount uint16 = pageHeaders["cell_count"]

		fmt.Printf("database page size: %v\n", pageSize)
		fmt.Printf("number of tables: %v\n", cellCount)

	case ".tables":
		dbFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		var pageIndex uint16 = 1
		pageSize := readPageSize(*dbFile)

		var pageHeaders map[string]uint16 = readPageHeader(*dbFile, pageSize, pageIndex)
		var cellCount uint16 = pageHeaders["cell_count"]

		cellPointers := readCellPointers(*dbFile, cellCount, pageSize, pageIndex)

		var records [][]byte
		var varIntSizes []int
		records, varIntSizes = readRecords(*dbFile, cellPointers, pageSize, pageIndex)
		var recordSizeMaps []map[string]uint16 = parseRecords(records, varIntSizes, cellPointers)
		for _, mp := range recordSizeMaps {
			for key, val := range mp {
				fmt.Println(key, ":", val)
			}
			fmt.Println("----------------")
		}

		var recordNameMaps []map[string]string = parseRecordsSizeMaps(records, varIntSizes, recordSizeMaps)

		for _, mp := range recordNameMaps {
			for key, val := range mp {
				fmt.Println(key, ":", val)
			}
			fmt.Println("----------------")
		}

		// readPage(*dbFile, pageSize, uint16(4))
		// fmt.Println(readPageHeader(*dbFile, pageSize, pageIndex))
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
