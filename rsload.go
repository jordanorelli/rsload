package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"runtime/pprof"
	"time"
)

var chunk_size = 100

var options struct {
	host     string
	port     int
	password string
	buffer   int
	pipe     bool
	profile  string
	verbose  bool
}

func usage(status int) {
	fmt.Println("usage: rsload [filename]")
	os.Exit(status)
}

func main() {
	flag.Parse()

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", options.host, options.port))
	if err != nil {
		fmt.Printf("unable to connect to redis: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	if options.password != "" {
		fmt.Fprintf(conn, "*2\r\n$4\r\nauth\r\n$%d\r\n%s\r\n", len(options.password), options.password)
		v, err := readValue(conn)
		if err != nil {
			fmt.Printf("unable to auth: %v\n", err)
			os.Exit(1)
		}
		if !isOK(v) {
			fmt.Printf("auth not OK: %q\n", v)
			os.Exit(1)
		}
	}

	if options.profile != "" {
		f, err := os.Create(options.profile)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	var infile *os.File

	if options.pipe {
		infile = os.Stdin
	} else {
		args := flag.Args()
		if len(args) < 1 {
			usage(1)
		}
		fname := args[0]

		var err error
		infile, err = os.Open(fname)
		if err != nil {
			fmt.Printf("unable to open file %s: %v\n", fname, err)
			os.Exit(1)
		}
		defer infile.Close()
	}

	type chunk struct {
		vals []value
		t    time.Time
	}
	c := make(chan maybe)
	sent := make(chan *chunk, 200)
	go streamValues(infile, c)
	go func() {
		w := bufio.NewWriterSize(conn, 16384)
		defer func() {
			close(sent)
			fmt.Println("All data transferred. Waiting for the last reply...")
		}()
		requests := &chunk{vals: make([]value, 0, chunk_size)}
		for m := range c {
			if !m.ok() {
				fmt.Fprintf(os.Stderr, "InputError: %v\n", m.err())
				continue
			}
			if _, err := m.val().Write(w); err != nil {
				fmt.Fprintf(os.Stderr, "WriteError: %v\n", err)
			}

			requests.vals = append(requests.vals, m.val())
			if len(requests.vals) == cap(requests.vals) {
				requests.t = time.Now()
				if err := w.Flush(); err != nil {
					fmt.Fprintf(os.Stderr, "FlushError: %v\n", err)
				}
				sent <- requests
				requests = &chunk{vals: make([]value, 0, chunk_size)}
			}
		}
		if len(requests.vals) > 0 {
			if err := w.Flush(); err != nil {
				fmt.Fprintf(os.Stderr, "FlushError: %v\n", err)
			}
			sent <- requests
		}
	}()

	replies, errors := 0, 0
	responses := make(chan maybe)
	go streamValues(conn, responses)
	id := 1
	for requests := range sent {
		for _, request := range requests.vals {
			response := <-responses
			if response.ok() {
				switch r := response.val().(type) {
				case ErrorVal:
					if options.verbose {
						fmt.Fprintf(os.Stderr, "%q -> %q\n", request, response.val())
					} else {
						fmt.Fprintln(os.Stderr, r)
					}
					errors++
				default:
					if options.verbose {
						fmt.Fprintf(os.Stdout, "%q -> %q\n", request, response.val())
					}
					replies++
				}
			} else {
				fmt.Fprintf(os.Stderr, "ResponseError: %v\n", response.err())
			}
		}
		elapsed := time.Since(requests.t)
		fmt.Fprintf(os.Stdout, "%d %d %v %v\n", id, len(requests.vals), elapsed,
			time.Duration(int64(elapsed)/int64(len(requests.vals))))
		id++
	}
	fmt.Println("Last reply received from server.")
	fmt.Printf("errors: %d, replies: %d\n", errors, replies)
}

func init() {
	flag.StringVar(&options.host, "h", "127.0.0.1", "hostname")
	flag.IntVar(&options.port, "p", 6379, "port")
	flag.StringVar(&options.password, "a", "", "password")
	flag.IntVar(&options.buffer, "buffer", 0, "number of outstanding statements allowed before throttling")
	flag.BoolVar(&options.pipe, "pipe", false, "transfers input from stdin to server")
	flag.StringVar(&options.profile, "profile", "", "pprof file output for performance debugging")
	flag.BoolVar(&options.verbose, "v", false, "verbose mode (prints all requests and responses)")
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
