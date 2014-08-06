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
	Write(io.Writer) (int, error)
}

func auth(password string) value {
    return Array{BulkString("auth"), BulkString(password)}
}

func isOK(v value) bool {
	vv, ok := v.(String)
	if !ok {
		return false
	}
	return vv == "OK"
}

func streamValues(r io.Reader, c chan value, e chan error) {
	defer close(c)
	defer close(e)

	r = bufio.NewReader(r)
	for {
		v, err := readValue(r)
		switch err {
		case io.EOF:
			return
		case nil:
			c <- v
		default:
			e <- err
		}
	}
}

func writeValues(w io.Writer, c chan value) {
	for v := range c {
		v.Write(w)
	}
}

func readValue(r io.Reader) (value, error) {
	var br *bufio.Reader
	switch t := r.(type) {
	case *bufio.Reader:
		br = t
	default:
		br = bufio.NewReader(r)
	}

	line, err := br.ReadBytes('\n')
	switch err {
	case io.EOF:
		if line != nil && len(line) > 0 {
			break
		}
		return nil, err
	case nil:
		break
	default:
		return nil, fmt.Errorf("unable to read value in redis protocol: %v")
	}

	if len(line) < 3 {
		return nil, fmt.Errorf("unable to read redis protocol value: input %q is too small", line)
	}
	if line[len(line)-2] != '\r' {
		return nil, fmt.Errorf("unable to read redis protocol value: bad line terminator: %q", line)
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
		return readBulkString(line[1:], br)
	case start_array:
		return readArray(line[1:], br)
	default:
		return nil, fmt.Errorf("unable to read redis protocol value: illegal start character: %c", line[0])
	}
}

// ------------------------------------------------------------------------------

type String string

func readString(b []byte) (value, error) {
	return String(b), nil
}

func (s String) Write(w io.Writer) (int, error) {
	return fmt.Fprintf(w, "+%s\r\n", s)
}

// ------------------------------------------------------------------------------

type Error string

func readError(b []byte) (value, error) {
	return Error(b), nil
}

func (e Error) Write(w io.Writer) (int, error) {
	return fmt.Fprintf(w, "-%s\r\n")
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

func (i Integer) Write(w io.Writer) (int, error) {
	return fmt.Fprintf(w, ":%d\r\n", i)
}

// ------------------------------------------------------------------------------

type BulkString string

func readBulkString(prefix []byte, r io.Reader) (value, error) {
	n, err := strconv.ParseInt(string(prefix), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to read bulkstring in redis protocol: bad prefix: %v", err)
	}

	switch {
	case n == -1:
		return nil, nil
	case n < 0:
		return nil, fmt.Errorf("redis protocol error: illegal bulk string of negative length %d", n)
	}

	n += 2
	b := make([]byte, n)
	n_read, err := r.Read(b)
	switch err {
	case io.EOF, nil:
		break
	default:
		return nil, fmt.Errorf("unable to read bulkstring in redis protocol: error on read: %v", err)
	}

	if int64(n_read) != n {
		return nil, fmt.Errorf("unable to read bulkstring in redis protocol: read %d bytes, expected to read %d bytes", int64(n_read), n)
	}

	if len(b) < 2 {
		return nil, fmt.Errorf("unable to read bulkstring in redis protocol: input %q is too short", b)
	}

	return BulkString(b[:len(b)-2]), nil
}

func (s BulkString) Write(w io.Writer) (int, error) {
	return fmt.Fprintf(w, "$%d\r\n%s\r\n", len(s), s)
}

// -----------------------------------------------------------------------------------------

type Array []value

func readArray(prefix []byte, r *bufio.Reader) (value, error) {
	n, err := strconv.ParseInt(string(prefix), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("unable to read array in redis protocol: bad prefix: %v", err)
	}

	switch {
	case n == -1:
		return nil, nil
	case n < 0:
		return nil, fmt.Errorf("redis protocol error: illegal array of negative length %d", n)
	}

	a := make(Array, n)
	for i := int64(0); i < n; i++ {
		v, err := readValue(r)
		if err != nil {
			return nil, fmt.Errorf("unable to read array value in redis protocol: %v", err)
		}
		a[i] = v
	}
	return a, nil
}

func (a Array) Write(w io.Writer) (int, error) {
	n, err := fmt.Fprintf(w, "*%d\r\n", len(a))
	if err != nil {
		return n, err
	}

	var (
		nn int
		e  error
	)
	for i := 0; i < len(a); i++ {
		nn, e = a[i].Write(w)
		n += nn
		if e != nil {
			return n, e
		}
	}
	return n, nil
}
