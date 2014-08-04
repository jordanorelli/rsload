package main

import (
	"fmt"
	"strings"
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

func readValue(b []byte) (value, error) {
	if len(b) < 2 {
		return nil, fmt.Errorf("unable to read redis protocol value: input is too small")
	}
	switch b[0] {
	case start_string:
		return readString(b[1:])
    case start_error:
        return readError(b[1:])
	default:
		return nil, fmt.Errorf("unable to read redis protocol value: illegal start character: %c", b[0])
	}
}

// ------------------------------------------------------------------------------

type String string

func readString(b []byte) (value, error) {
	return String(strings.Trim(string(b), "\r\n")), nil
}

// ------------------------------------------------------------------------------

type Error string

func readError(b []byte) (value, error) {
    return Error(strings.Trim(string(b), "\r\n")), nil
}










