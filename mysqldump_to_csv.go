package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/kumakichi/csv"
)

// Returns true if the line begins a SQL insert statement.
func isInsert(line string) bool {
	return strings.HasPrefix(line, "INSERT INTO")
}

// Returns the portion of an INSERT statement containing values
func getValues(line string) string {
	a := strings.Split(line, "` VALUES ")
	if len(a) != 2 {
		log.Fatal("parse mysqldump file fail\n")
	}
	return a[1]
}

// Ensures that values from the INSERT statement meet basic checks.
func valuesSanityCheck(values string) bool {
	if values == "" {
		return false
	}

	if values[0] != '(' {
		return false
	}

	return true
}

// Given a file handle and the raw values from a MySQL INSERT statement, write the equivalent CSV to the file
func parseValues(values string, outfile *os.File) {
	var latestRow []string

	csv.SetReadQuoteChar('\'')
	rows := strings.Split(values, "),")
	writer := csv.NewWriter(outfile)
	writer.UseCRLF = true

	for i, row := range rows {
		if i == len(rows)-1 {
			length := len(row)
			if length > 1 && row[length-1] == ';' && row[length-2] == ')' {
				row = row[:length-2]
			}
		}
		reader := csv.NewReader(strings.NewReader(row))
		reader.LazyQuotes = true

		a, err := reader.Read()
		if err != nil {
			log.Fatal(err)
		}

		for i := range a {
			column := a[i]

			// If our current string is empty...
			if len(column) == 0 {
				latestRow = append(latestRow, "")
				continue
			}

			// If our string starts with an open paren
			if column[0] == '(' {
				// If we're beginning a new row, eliminate the
				// opening parentheses.
				if len(latestRow) == 0 {
					column = column[1:]
				}
			}

			// Add our column to the row we're working on.
			latestRow = append(latestRow, column)
		}

		if len(latestRow) > 0 {
			err := writer.Write(latestRow)
			if err != nil {
				log.Fatal(err)
			}
			latestRow = make([]string, 0)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		log.Fatal(err)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s file\n", os.Args[0])
		os.Exit(0)
	}

	ch := make(chan struct{})
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)
	go procMysqldumpFile(ch)

	select {
	case <-sc:
		os.Exit(0)
	case <-ch:
		os.Exit(0)
	}
}

func procMysqldumpFile(ch chan struct{}) {
	defer func() {
		ch <- struct{}{}
	}()

	file, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Println("Open input file failed : ", os.Args[1])
		return
	}
	defer file.Close()

	br := bufio.NewReaderSize(file, 1024*1024*1)
	for {
		line, isPrefix, err := br.ReadLine()
		if err != nil {
			if err != io.EOF {
				log.Fatal(err)
			}
			break
		}

		if isPrefix {
			fmt.Println("A too long line, seems unexpected")
			return
		}

		lineStr := string(line)
		var values string
		// Look for an INSERT statement and parse it.
		if isInsert(lineStr) {
			values = getValues(lineStr)
		}
		if valuesSanityCheck(values) {
			parseValues(values, os.Stdout)
		}
	}
}
