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

var chunk_target = 250 * time.Millisecond
var chunk_max = 10000

var options struct {
	host      string // hostname or ip address of the redis server to connect to
	port      int    // redis port to connect to
	password  string // redis password used with redis auth
	pipe      bool   // whether or not to take input from stdin
	profile   string // path to which a cpu profile will be written, used for debug purposes
	verbose   bool   // whether or not to echo out request/response pairs
	chunkInfo bool   // whether or not to write out stats about each chunk sent
}

func usage(status int) {
	fmt.Println("usage: rsload [filename]")
	os.Exit(status)
}

// sets up our input file for reading.  If the --pipe option was specified, the
// input file is stdin. Otherwise, the input file is the first argument on the
// command line.
func infile() *os.File {
	if options.pipe {
		return os.Stdin
	}

	args := flag.Args()
	if len(args) < 1 {
		usage(1)
	}
	fname := args[0]

	f, err := os.Open(fname)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to open file %s: %v\n", fname, err)
		os.Exit(1)
	}
	return f
}

func connect() net.Conn {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", options.host, options.port))
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to connect to redis: %v\n", err)
		os.Exit(1)
	}
	auth(conn)
	return conn
}

func auth(c net.Conn) {
	if options.password == "" {
		return
	}

	fmt.Fprintf(c, "*2\r\n$4\r\nauth\r\n$%d\r\n%s\r\n", len(options.password), options.password)
	v, err := readValue(c)
	if err != nil {
		fmt.Printf("unable to auth: %v\n", err)
		os.Exit(1)
	}
	if !isOK(v) {
		fmt.Printf("auth not OK: %q\n", v)
		os.Exit(1)
	}
}

func main() {
	flag.Parse()

	conn := connect()
	defer conn.Close()

	if options.profile != "" {
		f, err := os.Create(options.profile)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	f := infile()

	responses := make(chan maybe, chunk_max)
	go streamValues(conn, responses)

	c := make(chan maybe, chunk_max)
	go streamValues(f, c)

	w := bufio.NewWriterSize(conn, 16384)

	requests := newChunk(1)
	errors, replies := 0, 0
	for m := range c {
		if !m.ok() {
			fmt.Fprintf(os.Stderr, "InputError: %v\n", m.err())
			continue
		}
		requests.vals = append(requests.vals, m.val())
		if len(requests.vals) == cap(requests.vals) {
			stats := requests.send(w, responses)
			stats.log()
			errors += stats.errors
			replies += stats.replies
			time.Sleep(time.Duration(float64(stats.elapsed) * 0.1))
			requests = newChunk(stats.nextSize())
		}
	}
	if len(requests.vals) > 0 {
		stats := requests.send(w, responses)
		stats.log()
		errors += stats.errors
		replies += stats.replies
	}

	fmt.Println("Last reply received from server.")
	fmt.Printf("errors: %d, replies: %d\n", errors, replies)
}

func init() {
	flag.StringVar(&options.host, "h", "127.0.0.1", "hostname")
	flag.IntVar(&options.port, "p", 6379, "port")
	flag.StringVar(&options.password, "a", "", "password")
	flag.BoolVar(&options.pipe, "pipe", false, "transfers input from stdin to server")
	flag.StringVar(&options.profile, "profile", "", "pprof file output for performance debugging")
	flag.BoolVar(&options.verbose, "v", false, "verbose mode (prints all requests and responses)")
	flag.BoolVar(&options.chunkInfo, "chunk-info", false, "show chunk info")
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
