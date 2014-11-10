package main

import (
	"bufio"
	"fmt"
	"os"
	"sync"
	"time"
)

var chunk_id struct {
	sync.Mutex
	val int
}

// type chunk represents a grouping of redis statements to be sent to a redis
// server.
type chunk struct {
	id   int
	vals []value
	t    time.Time
}

// creates a new chunk of a given size, automatically assigning it a
// monotomically increasing id.
func newChunk(size int) *chunk {
	chunk_id.Lock()
	defer chunk_id.Unlock()
	chunk_id.val++
	return &chunk{id: chunk_id.val, vals: make([]value, 0, size)}
}

// write out a chunk's statements onto a bufio.Writer.  After sending all
// statements onto the bufio.Writer, reads from the provided channel of
// responses N times, where N is the number of statements originally sent.
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

// -----------------------------------------------------------------------------

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
	return min(target, options.chunkMax, 2*s.read)
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
