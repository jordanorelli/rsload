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
	host      string
	port      int
	password  string
	buffer    int
	pipe      bool
	profile   string
	verbose   bool
	chunkInfo bool
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

type sendResult struct {
	read     int           // number of statement sread on the incoming statement stream
	errors   int           // number of error responses seen on the redis response stream
	replies  int           // number of reply responses seen on the redis response stream
	byteSize int           // number of bytes sent
	elapsed  time.Duration // time taken to send requests and receive responses from redis
	paused   time.Duration // sleep time inserted

	startTime time.Time
}

func (s *sendResult) start() {
	s.startTime = time.Now()
}

func (s *sendResult) stop() {
	s.elapsed = time.Since(s.startTime)
}

func (s *sendResult) avg() time.Duration {
	if s.read == 0 {
		fmt.Fprintln(os.Stderr, "for some reason, we tried to divide by zero on sendResult.avg.  we recovered, though.")
		return 100 * time.Microsecond
	}
	return time.Duration(int64(s.elapsed) / int64(s.read))
}

func (s *sendResult) log() {
	if !options.chunkInfo {
		return
	}
	fmt.Printf("errors: %d replies: %d total: %d sent: %d elapsed: %v avg: %v size: %v\n", s.errors, s.replies, s.errors+s.replies, s.read, s.elapsed, s.avg(), s.byteSize)
}

func (s *sendResult) nextSize() int {
	target := int(int64(chunk_target) / int64(s.avg()))
	return min(target, chunk_max, 2*s.read)
}

func min(vals ...int) int {
	var best int
	switch len(vals) {
	case 0:
		return 0
	case 1:
		return vals[0]
	default:
		best = vals[0]
	}
	for _, v := range vals[1:] {
		if v < best {
			best = v
		}
	}
	return best
}

func (s *sendResult) accumulate(request value, response maybe) {
	s.read++
	if response.ok() {
		switch r := response.val().(type) {
		case ErrorVal:
			// these errors are errors reported from redis. e.g., if you
			// try to do something like delete a key that doesn't exist or
			// send an invalid command to redis
			if options.verbose {
				fmt.Fprintf(os.Stderr, "%q -> %q\n", request, response.val())
			} else {
				fmt.Fprintf(os.Stderr, "%q\n", r)
			}
			s.errors++
		default:
			if options.verbose {
				fmt.Fprintf(os.Stdout, "%q -> %q\n", request, response.val())
			}
			s.replies++
		}
	} else {
		// we get here when we encounter an error in the response stream.
		// That is, the response stream contains bytes that are not valid
		// in the redis protocol.
		fmt.Fprintf(os.Stderr, "ResponseError: %v\n", response.err())
	}
}

func (c *chunk) send(w *bufio.Writer, responses chan maybe) *sendResult {
	stats := new(sendResult)
	stats.start()
	defer stats.stop()

	for _, v := range c.vals {
		n, err := v.Write(w)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error writing a statement: %v\n", err)
		}
		stats.byteSize += n
	}
	if err := w.Flush(); err != nil {
		fmt.Fprintf(os.Stderr, "error flushing statement buffer: %v\n", err)
	}

	for _, request := range c.vals {
		response, ok := <-responses
		if !ok {
			// we get here when the response channel closes too early
			fmt.Fprintf(os.Stderr, "error reading redis response stream: response chan closed early")
			return stats
		}
		stats.accumulate(request, response)
	}

	return stats
}

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

	f := infile()

	responses := make(chan maybe)
	go streamValues(conn, responses)

	c := make(chan maybe)
	go streamValues(f, c)

	w := bufio.NewWriterSize(conn, 16384)

	id := 1
	requests := &chunk{id: id, vals: make([]value, 0, chunk_size)}
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
			id++
			requests = &chunk{id: id, vals: make([]value, 0, stats.nextSize())}
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
	flag.IntVar(&options.buffer, "buffer", 0, "number of outstanding statements allowed before throttling")
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
