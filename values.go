package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

var (
	start_string     byte = '+'
	start_error      byte = '-'
	start_integer    byte = ':'
	start_bulkstring byte = '$'
	start_array      byte = '*'
)

type value interface {
}

func readValue(r io.Reader) (value, error) {
	br := bufio.NewReader(r)
	line, err := br.ReadBytes('\n')
	switch err {
	case io.EOF:
		if line != nil {
			break
		}
		return nil, err
	case nil:
		break
	default:
		return nil, fmt.Errorf("unable to read value in redis protocol: %v")
	}

	if len(line) < 3 {
		return nil, fmt.Errorf("unable to read redis protocol value: input is too small")
	}
	if line[len(line)-2] != '\r' {
		return nil, fmt.Errorf("unable to read redis protocol value: bad line terminator")
	}
	line = line[:len(line)-2]
	switch line[0] {
	case start_string:
		return readString(line[1:])
	case start_error:
		return readError(line[1:])
	case start_integer:
		return readInteger(line[1:])
	case start_bulkstring:
		return readBulkString(line[1:], r)
	default:
		return nil, fmt.Errorf("unable to read redis protocol value: illegal start character: %c", line[0])
	}
}

// ------------------------------------------------------------------------------

type String string

func readString(b []byte) (value, error) {
	return String(b), nil
}

// ------------------------------------------------------------------------------

type Error string

func readError(b []byte) (value, error) {
	return Error(b), nil
}

// ------------------------------------------------------------------------------

type Integer int64

func readInteger(b []byte) (value, error) {
	i, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to read integer in redis protocol format: %v", err)
	}
	return Integer(i), nil
}

// -----------------------------------------------------------------------------------------

type BulkString string

func readBulkString(prefix []byte, r io.Reader) (value, error) {
	return nil, nil
}
