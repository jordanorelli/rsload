package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
)

type statement struct {
	head *node
	tail *node
}

func (s statement) String() string {
	var buf bytes.Buffer
	s.each(func(n *node) error {
        _, err := fmt.Fprint(&buf, n)
        return err
	})
	return buf.String()
}

func (s *statement) each(fn func(*node) error) error {
	curr := s.head
	for curr != nil {
        err := fn(curr)
        if err != nil {
            return err
        }
		curr = curr.next
	}
    return nil
}

func (s *statement) write(w io.Writer) error {
    return s.each(func(n *node) error {
        _, err := w.Write(n.line)
        return err
    })
}

type node struct {
	line []byte
	next *node
}

func (n node) String() string {
	return fmt.Sprintf("%s", n.line)
}

func (s *statement) add(line []byte) {
	n := &node{line, nil}
	if s.head == nil {
		s.head = n
	} else if s.head.next == nil {
        s.head.next = n
    }
	if s.tail != nil {
		s.tail.next = n
	}
    s.tail = n
}

func whitespace(buf []byte) bool {
    for i, _ := range buf {
        switch buf[i] {
        case '\r', '\n':
        default:
            return false
        }
    }
    return true
}

func split(r io.Reader, c chan statement) {
	defer close(c)
	br := bufio.NewReader(r)
	var s *statement
	for {
		line, err := br.ReadBytes('\n')
		switch err {
		case nil:
		case io.EOF:
            if s != nil {
                c <- *s
            }
			return
		default:
			fmt.Printf("error on read: %v\n", err)
			return
		}
        if whitespace(line) {
            continue
        }
		if line[len(line)-2] != '\r' {
			fmt.Printf("bad line terminator")
			break
		}
		if line[0] == '*' {
            if s != nil {
                c <- *s
            }
            s = new(statement)
            s.add(line)
		} else {
			if s == nil {
				fmt.Println("ummm wut")
				return
			}
			s.add(line)
		}
	}
}
