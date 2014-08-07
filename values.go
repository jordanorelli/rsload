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
	return string(vv) == "OK"
}

func getBytes(v value) []byte {
	var buf bytes.Buffer
	v.Write(&buf)
	return buf.Bytes()
}

func streamValues(r io.Reader, c chan maybe) {
	defer close(c)

	r = bufio.NewReader(r)
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
	switch line[0] {
	case start_string:
		return StringVal(line), nil
	case start_error:
		return ErrorVal(line), nil
	case start_integer:
		line = line[:len(line)-2]
		return Integer(line[1:]), nil
	case start_bulkstring:
		line = line[:len(line)-2]
		return readBulkString(line[1:], br)
	case start_array:
		line = line[:len(line)-2]
		return readArray(line[1:], br)
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
	b[0] = '+'
	copy(b[1:], []byte(s))
	b[len(b)-2] = '\r'
	b[len(b)-1] = '\n'
	return b
}

// ------------------------------------------------------------------------------

type ErrorVal []byte

func (e ErrorVal) Write(w io.Writer) (int, error) {
	// w.Write([]byte{'-'})
	// w.Write([]byte(e))
	// w.Write([]byte{'\r', '\n'})
	// return 0, nil
	return w.Write(e)
}

func Error(s string) value {
	b := make(ErrorVal, len(s)+3)
	b[0] = '-'
	copy(b[1:], []byte(s))
	b[len(b)-2] = '\r'
	b[len(b)-1] = '\n'
	return b
}

// ------------------------------------------------------------------------------

type Integer []byte

func (i Integer) Write(w io.Writer) (int, error) {
	w.Write([]byte{':'})
	w.Write(i)
	w.Write([]byte{'\r', '\n'})
	return 0, nil
}

// ------------------------------------------------------------------------------

type BulkString []byte

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
	b := make(BulkString, n)
	n_read, err := io.ReadFull(r, b)
	switch err {
	case io.EOF:
		fmt.Printf("saw eof after %d bytes looking for %d bytes in bulkstring\n", n_read, n)
		fmt.Println(string(b))
	case nil:
		break
	default:
		return nil, fmt.Errorf("unable to read bulkstring in redis protocol: error on read: %v", err)
	}

	return b[:len(b)-2], nil
}

func (s BulkString) Write(w io.Writer) (int, error) {
	w.Write([]byte{'$'})
	w.Write([]byte(strconv.Itoa(len(s))))
	w.Write([]byte{'\r', '\n'})
	w.Write(s)
	w.Write([]byte{'\r', '\n'})
	return 0, nil
}

// -----------------------------------------------------------------------------------------

type Array []byte

func readArray(prefix []byte, r *bufio.Reader) (value, error) {
	n, err := strconv.Atoi(string(prefix))
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
	buf.Write([]byte{'*'})
	buf.Write([]byte(strconv.Itoa(n)))
	buf.Write([]byte{'\r', '\n'})
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
	w.Write(a)
	return 0, nil
}
