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

var chunk_size = 1
var chunk_target = 250 * time.Millisecond
var chunk_max = 10000

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

type chunk struct {
	id   int
	vals []value
	t    time.Time
}

func (c *chunk) send(w *bufio.Writer, responses chan maybe) {
	start := time.Now()
	for _, v := range c.vals {
		v.Write(w)
	}
	size := w.Buffered()
	w.Flush()
	errors, replies := 0, 0
	for _, request := range c.vals {
		response, ok := <-responses
		if !ok {
			fmt.Fprintf(os.Stderr, "ohhhhhhhhhhhhh fuck\n")
			return
		}
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
	elapsed := time.Since(start)
	sleep := elapsed / 4
	time.Sleep(sleep)
	avg := time.Duration(int64(elapsed) / int64(len(c.vals)))
	next_size := int(int64(chunk_target) / int64(avg))
	if true {
		fmt.Printf("id: %d errors: %d replies: %d total: %d sent: %d elapsed: %v avg: %v size: %v sleep: %v next_size: %v\n",
			c.id, errors, replies, errors+replies, len(c.vals), elapsed, avg, size, sleep, next_size)
	}
	if next_size < chunk_size*2 {
		chunk_size = next_size
	} else {
		chunk_size *= 2
	}
	if chunk_size > chunk_max {
		chunk_size = chunk_max
	}
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

	responses := make(chan maybe)
	go streamValues(conn, responses)

	c := make(chan maybe)
	go streamValues(infile, c)

	w := bufio.NewWriterSize(conn, 16384)

	id := 1
	requests := &chunk{id: id, vals: make([]value, 0, chunk_size)}
	for m := range c {
		if !m.ok() {
			fmt.Fprintf(os.Stderr, "InputError: %v\n", m.err())
			continue
		}
		requests.vals = append(requests.vals, m.val())
		if len(requests.vals) == cap(requests.vals) {
			requests.send(w, responses)
			id++
			requests = &chunk{id: id, vals: make([]value, 0, chunk_size)}
		}
	}
	if len(requests.vals) > 0 {
		requests.send(w, responses)
	}

	// fmt.Println("Last reply received from server.")
	// fmt.Printf("errors: %d, replies: %d\n", errors, replies)
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
