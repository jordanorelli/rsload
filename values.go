package main

import (
	"bufio"
	"bytes"
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

type maybe struct {
	value
	error
}

func (m maybe) err() error {
	return m.error
}

func (m maybe) ok() bool {
	return m.error == nil
}

func (m maybe) val() value {
	return m.value
}

func isOK(v value) bool {
	vv, ok := v.(StringVal)
	if !ok {
		return false
	}
	return string(vv) == "+OK\r\n"
}

func getBytes(v value) []byte {
	var buf bytes.Buffer
	v.Write(&buf)
	return buf.Bytes()
}

func streamValues(r io.Reader, c chan maybe) {
	defer close(c)

	r = bufio.NewReaderSize(r, 65536)
	for {
		v, err := readValue(r)
		switch err {
		case io.EOF:
			return
		case nil:
			c <- maybe{value: v}
		default:
			c <- maybe{error: err}
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
		br = bufio.NewReaderSize(r, 65536)
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
	switch line[0] {
	case start_string:
		return StringVal(line), nil
	case start_error:
		return ErrorVal(line), nil
	case start_integer:
		return IntVal(line), nil
	case start_bulkstring:
		return readBulkString(line, br)
	case start_array:
		return readArray(line, br)
	default:
		return nil, fmt.Errorf("unable to read redis protocol value: illegal start character: %c", line[0])
	}
}

// ------------------------------------------------------------------------------

type StringVal []byte

func (s StringVal) Write(w io.Writer) (int, error) {
	return w.Write(s)
}

func String(s string) value {
	b := make(StringVal, len(s)+3)
	b[0] = start_string
	copy(b[1:], s)
	b[len(b)-2] = '\r'
	b[len(b)-1] = '\n'
	return b
}

// ------------------------------------------------------------------------------

type ErrorVal []byte

func (e ErrorVal) Write(w io.Writer) (int, error) {
	return w.Write(e)
}

func Error(s string) value {
	b := make(ErrorVal, len(s)+3)
	b[0] = start_error
	copy(b[1:], s)
	b[len(b)-2] = '\r'
	b[len(b)-1] = '\n'
	return b
}

// ------------------------------------------------------------------------------

type IntVal []byte

func (i IntVal) Write(w io.Writer) (int, error) {
	return w.Write(i)
}

func Int(i int) value {
	s := strconv.Itoa(i)
	b := make(ErrorVal, len(s)+3)
	b[0] = start_integer
	copy(b[1:], s)
	b[len(b)-2] = '\r'
	b[len(b)-1] = '\n'
	return b
}

// ------------------------------------------------------------------------------

type BulkStringVal []byte

func BulkString(s string) value {
	l := strconv.Itoa(len(s))
	b := make(BulkStringVal, len(l)+len(s)+5)
	b[0] = '$'
	copy(b[1:], l)
	b[len(l)+1] = '\r'
	b[len(l)+2] = '\n'
	copy(b[len(l)+3:], s)
	b[len(b)-2] = '\r'
	b[len(b)-1] = '\n'
	return b
}

func readBulkString(prefix []byte, r io.Reader) (value, error) {
	n, err := strconv.Atoi(string(prefix[1 : len(prefix)-2]))
	if err != nil {
		return nil, fmt.Errorf("unable to read bulkstring in redis protocol: bad prefix: %v", err)
	}

	switch {
	case n == -1:
		return nil, nil
	case n < 0:
		return nil, fmt.Errorf("redis protocol error: illegal bulk string of negative length %d", n)
	}

	b := make(BulkStringVal, len(prefix)+n+2)
	copy(b, prefix)
	n_read, err := io.ReadFull(r, b[len(prefix):])
	switch err {
	case io.EOF:
		fmt.Printf("saw eof after %d bytes looking for %d bytes in bulkstring\n", n_read, n)
		fmt.Println(string(b))
	case nil:
		break
	default:
		return nil, fmt.Errorf("unable to read bulkstring in redis protocol: error on read: %v", err)
	}

	return b, nil
}

func (s BulkStringVal) Write(w io.Writer) (int, error) {
	return w.Write(s)
}

// -----------------------------------------------------------------------------------------

type Array []byte

func readArray(prefix []byte, r *bufio.Reader) (value, error) {
	n, err := strconv.Atoi(string(prefix[1 : len(prefix)-2]))
	if err != nil {
		return nil, fmt.Errorf("unable to read array in redis protocol: bad prefix: %v", err)
	}

	switch {
	case n == -1:
		return nil, nil
	case n < 0:
		return nil, fmt.Errorf("redis protocol error: illegal array of negative length %d", n)
	}

	buf := bytes.NewBuffer(make([]byte, 0, n*128))
	buf.Write(prefix)
	for i := 0; i < n; i++ {
		v, err := readValue(r)
		if err != nil {
			return nil, fmt.Errorf("unable to read array value in redis protocol: %v", err)
		}
		v.Write(buf)
	}
	return Array(buf.Bytes()), nil
}

func (a Array) Write(w io.Writer) (int, error) {
	return w.Write(a)
}
