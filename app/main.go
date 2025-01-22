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

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		header := make([]byte, 100)

		_, err = databaseFile.Read(header)
		if err != nil {
			log.Fatal(err)
		}

		var pageSize uint16
		if err := binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &pageSize); err != nil {
			fmt.Println("Failed to read integer:", err)
			return
		}

		// Reading Cells in Page that Indicate Table numbers
		pageHeader := make([]byte, pageSize)

		_, err = databaseFile.ReadAt(pageHeader, 100)
		if err != nil {
			log.Fatal(err)
		}

		var cellCount uint16
		if err := binary.Read(bytes.NewReader(pageHeader[3:5]), binary.BigEndian, &cellCount); err != nil {
			fmt.Println("Failed to read Cell Count", err)
			return
		}

		fmt.Printf("database page size: %v\n", pageSize)
		fmt.Printf("number of tables: %v\n", cellCount)
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
