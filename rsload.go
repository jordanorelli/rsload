package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"strings"
)

var (
	host = "localhost"
	port = 6379
)

func usage(status int) {
	fmt.Println("usage: rsload [filename]")
	os.Exit(status)
}

func randomString(n int) string {
	var alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	buf := make([]byte, n)
	for i := 0; i < len(buf); i++ {
		buf[i] = alpha[rand.Intn(len(alpha)-1)]
	}
	return string(buf)
}

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		usage(1)
	}
	fname := args[0]

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		fmt.Printf("unable to connect to redis: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	f, err := os.Open(fname)
	if err != nil {
		fmt.Printf("unable to open file %s: %v\n", fname, err)
		os.Exit(1)
	}
	defer f.Close()

	c := make(chan statement)
	go split(f, c)

	s := randomString(32)
	go func() {
		for s := range c {
			if err := s.write(conn); err != nil {
				fmt.Println(err)
				break
			}
		}
		fmt.Fprintf(conn, "*2\r\n$4\r\necho\r\n$32\r\n%s\r\n", s)
	}()

	r := bufio.NewReader(conn)
	for {
		line, err := r.ReadString('\n')
		switch err {
		case nil:
			if strings.TrimSpace(line) == s {
				return
			}
		case io.EOF:
			return
		default:
			fmt.Println(err)
		}
	}
}
