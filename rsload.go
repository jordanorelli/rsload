package main

import (
	// "bufio"
	"flag"
	"fmt"
	// "io"
	"math/rand"
	"net"
	"os"
	// "strings"
)

var options struct {
	host     string
	port     int
	password string
}

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

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", options.host, options.port))
	if err != nil {
		fmt.Printf("unable to connect to redis: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	if options.password != "" {
		auth(options.password).Write(conn)
		v, err := readValue(conn)
		if err != nil {
			fmt.Printf("unable to auth: %v\n", err)
			os.Exit(1)
		}
		if !isOK(v) {
			fmt.Printf("not OK: %v\n", v)
			os.Exit(1)
		}
	}

	infile, err := os.Open(fname)
	if err != nil {
		fmt.Printf("unable to open file %s: %v\n", fname, err)
		os.Exit(1)
	}
	defer infile.Close()

	c := make(chan maybe)
	sent := make(chan value)
	go streamValues(infile, c)
	go func() {
		defer close(sent)
		for r := range c {
			if r.ok() {
				r.val().Write(conn)
				sent <- r.val()
			} else {
				// this bad
				fmt.Println(r.err())
			}
		}
	}()

	responses := make(chan maybe)
	go streamValues(conn, responses)
	for request := range sent {
		response := <-responses
		if response.ok() {
			fmt.Fprintf(os.Stdout, "%v +> %v\n", request, response.val())
		} else {
			fmt.Fprintf(os.Stderr, "%v -> %v\n", request, response.val())
		}
	}
}

func init() {
	flag.StringVar(&options.host, "h", "127.0.0.1", "hostname")
	flag.IntVar(&options.port, "p", 6379, "port")
	flag.StringVar(&options.password, "a", "", "password")
}

/*
  -h <hostname>     Server hostname (default: 127.0.0.1)
  -p <port>         Server port (default: 6379)
  -s <socket>       Server socket (overrides hostname and port)
  -a <password>     Password to use when connecting to the server
  -r <repeat>       Execute specified command N times
  -i <interval>     When -r is used, waits <interval> seconds per command.
                    It is possible to specify sub-second times like -i 0.1
  -n <db>           Database number
  -x                Read last argument from STDIN
  -d <delimiter>    Multi-bulk delimiter in for raw formatting (default: \n)
  -c                Enable cluster mode (follow -ASK and -MOVED redirections)
  --raw             Use raw formatting for replies (default when STDOUT is
                    not a tty)
  --latency         Enter a special mode continuously sampling latency
  --latency-history Like --latency but tracking latency changes over time.
                    Default time interval is 15 sec. Change it using -i.
  --slave           Simulate a slave showing commands received from the master
  --rdb <filename>  Transfer an RDB dump from remote server to local file.
  --pipe            Transfer raw Redis protocol from stdin to server
  --bigkeys         Sample Redis keys looking for big keys
  --eval <file>     Send an EVAL command using the Lua script at <file>
  --help            Output this help and exit
  --version         Output version and exit
*/
