package main

import (
	"flag"
	"fmt"
	"net"
	"os"
)

var options struct {
	host     string
	port     int
	password string
	buffer   int
	pipe     bool
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

	c := make(chan maybe)
	sent := make(chan value, options.buffer)
	go streamValues(infile, c)
	go func() {
		defer func() {
			close(sent)
			fmt.Println("All data transferred. Waiting for the last reply...")
		}()
		for r := range c {
			if r.ok() {
				r.val().Write(conn)
				sent <- r.val()
			} else {
				fmt.Fprintf(os.Stderr, "InputError: %v\n", r.err())
				return
			}
		}
	}()

	replies, errors := 0, 0
	responses := make(chan maybe)
	go streamValues(conn, responses)
	for _ = range sent {
		response := <-responses
		if response.ok() {
			switch r := response.val().(type) {
			case Error:
				fmt.Fprintln(os.Stderr, r)
				errors++
			default:
				replies++
			}
		} else {
			fmt.Fprintf(os.Stderr, "ResponseError: %v\n", response.err())
		}
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
