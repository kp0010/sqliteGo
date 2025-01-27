package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strings"
)

type RecordHeader struct {
	rowId            uint64
	cellPointer      uint16
	recordHeaderSize int8
	columnSizes      []uint16
}

func (recHead RecordHeader) String() string {
	columnSizesStr := strings.Trim(fmt.Sprint(recHead.columnSizes), "[]")

	return fmt.Sprintf(
		"RecordHeader{rowId: %d, cellPointer: %d, recordHeaderSize: %d, columnSizes: [%s]}",
		recHead.rowId, recHead.cellPointer, recHead.recordHeaderSize, columnSizesStr,
	)
}

type Row struct {
	rowId            uint64
	cellPointer      uint16
	recordHeaderSize int8
	columns          []interface{}
}

func (row Row) String() string {
	values := strings.Trim(fmt.Sprint(row.columns), "[]")

	return fmt.Sprintf(
		"Row{rowId: %d, cellPointer: %d, recordHeaderSize: %d\ncolumns: [%s]}",
		row.rowId, row.cellPointer, row.recordHeaderSize, values,
	)
}

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

	var res = make(map[string]uint16, 7)
	res = map[string]uint16{
		"page_index": pageIndex[0],
	}

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

		var offset int64 = 0
		if len(pageIndex) > 0 {
			offset += int64((pageIndex[0] - 1) * pageSize)
		}

		_, err := dbFile.ReadAt(recordBytes, int64(pointer)+offset)
		if err != nil {
			log.Fatal(err)
		}

		recordSize, size := parseVarint(recordBytes)

		recordBytes = make([]byte, int(recordSize)+1+size)
		_, err = dbFile.ReadAt(recordBytes, int64(pointer)+offset)
		if err != nil {
			log.Fatal(err)
		}

		records[i] = recordBytes
		varIntSizes[i] = size
	}

	return records, varIntSizes
}

func parseRecordHeader(records [][]byte, varIntSizes []int, cellPointers []uint16) []RecordHeader {
	// Record Header format
	// type RecordHeader struct {
	// 	rowId            uint64
	// 	cellPointer      uint16
	// 	recordHeaderSize int8
	// 	columnSizes      []uint16
	// }

	recordHeaders := make([]RecordHeader, 0, len(records))

	for idx, record := range records {
		varIntSize := varIntSizes[idx]

		rowId, rowIdLen := parseVarint(record[varIntSize:])

		var recordHeader = RecordHeader{
			rowId:            rowId,
			recordHeaderSize: int8(record[varIntSize+rowIdLen]),
			cellPointer:      uint16(cellPointers[idx]),
			columnSizes:      make([]uint16, 0, record[varIntSize+rowIdLen]-1),
		}

		i := varIntSize + rowIdLen + 1

		for i < int(recordHeader.recordHeaderSize+int8(varIntSize)+1) {
			varint, size := parseVarint(record[i:])
			i += size
			recordHeader.columnSizes = append(recordHeader.columnSizes, uint16(varint))
		}
		fmt.Println(recordHeader)
		fmt.Println("----------------")
		recordHeaders = append(recordHeaders, recordHeader)
	}

	return recordHeaders
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

func convSerialToValue(serialVal uint16, data []byte) interface{} {
	reader := bytes.NewReader(data)

	switch serialVal {

	case 0:
		return nil

	case 1:
		var res int8
		if err := binary.Read(reader, binary.BigEndian, &res); err != nil {
			fmt.Println("Failed to Convert to Value", err)
			return nil
		}
		return res

	case 2:
		var res int16
		if err := binary.Read(reader, binary.BigEndian, &res); err != nil {
			fmt.Println("Failed to Convert to Value", err)
			return nil
		}
		return res

	case 3:
		var res int32
		padding := append(make([]byte, 8), data...)
		reader := bytes.NewReader(padding)
		if err := binary.Read(reader, binary.BigEndian, &res); err != nil {
			fmt.Println("Failed to Convert to Value", err)
			return nil
		}
		return res

	case 4:
		var res int32
		if err := binary.Read(reader, binary.BigEndian, &res); err != nil {
			fmt.Println("Failed to Convert to Value", err)
			return nil
		}
		return res

	case 5:
		var res int64
		padding := append(make([]byte, 16), data...)
		reader := bytes.NewReader(padding)
		if err := binary.Read(reader, binary.BigEndian, &res); err != nil {
			fmt.Println("Failed to Convert to Value", err)
			return nil
		}
		return res

	case 6:
		var res int64
		if err := binary.Read(reader, binary.BigEndian, &res); err != nil {
			fmt.Println("Failed to Convert to Value", err)
			return nil
		}
		return res

	case 7:
		var res float64
		if err := binary.Read(reader, binary.BigEndian, &res); err != nil {
			fmt.Println("Failed to Convert to Value", err)
			return nil
		}
		return res

	case 8 | 9:
		return (map[bool]int{true: 1, false: 0}[serialVal == 9])

	default:
		return string(data)

	}
}

func readRows(records [][]byte, varIntSizes []int, recordHeaders []RecordHeader) []Row {
	// Row format
	// type Row struct {
	// 	rowId            uint64
	// 	cellPointer      uint16
	// 	recordHeaderSize int8
	// 	columns          []interface
	// }

	recordRows := make([]Row, len(recordHeaders))

	var offset uint16

	for idx, recordHeader := range recordHeaders {
		recordRows[idx] = Row{
			rowId:            recordHeader.rowId,
			recordHeaderSize: recordHeader.recordHeaderSize,
			cellPointer:      recordHeader.cellPointer,
			columns:          make([]interface{}, len(recordHeader.columnSizes)),
		}

		offset = uint16(varIntSizes[idx]) + uint16(recordHeader.recordHeaderSize)

		for col, size := range recordHeader.columnSizes {
			serialSize := convSizeToSerialType(size)
			res := convSerialToValue(size, records[idx][offset+1:offset+serialSize+1])
			offset += serialSize

			recordRows[idx].columns[col] = res
		}
	}

	return recordRows
}

func readPage(dbFile os.File, pageSize uint16, pageIndex uint16) []Row {

	var pageHeaders map[string]uint16 = readPageHeader(dbFile, pageSize, pageIndex)
	var cellCount uint16 = pageHeaders["cell_count"]

	cellPointers := readCellPointers(dbFile, cellCount, pageSize, pageIndex)

	var records [][]byte
	var varIntSizes []int

	records, varIntSizes = readRecords(dbFile, cellPointers, pageSize, pageIndex)

	var recordHeaders []RecordHeader = parseRecordHeader(records, varIntSizes, cellPointers)

	for _, mp := range recordHeaders {
		if 0 == 1 {
			fmt.Println(mp)
		}
	}

	var recordRows []Row = readRows(records, varIntSizes, recordHeaders)
	//
	for _, mp := range recordRows {
		fmt.Println(mp)
		fmt.Println("----------------")
	}
	return recordRows
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

		pageIndex := uint16(1)
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

		pageIndex := uint16(5)
		pageSize := readPageSize(*dbFile)

		schemaMaps := readPage(*dbFile, pageSize, 1)
		tableMaps := readPage(*dbFile, pageSize, pageIndex)

		if 0 == 1 {
			fmt.Println(schemaMaps, pageIndex, tableMaps)
		}

	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
