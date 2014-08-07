package main

import (
	"strings"
	"testing"
)

type valueTest struct {
	in  string
	out value
}

func eq(v1, v2 value) bool {
	if v1 == nil {
		return v2 == nil
	} else if v2 == nil {
		return false
	}

	b1, b2 := getBytes(v1), getBytes(v2)
	if len(b1) != len(b2) {
		return false
	}
	for i := 0; i < len(b1); i++ {
		if b1[i] != b2[i] {
			return false
		}
	}
	return true
}

func (test valueTest) run(t *testing.T) {
	v, err := readValue(strings.NewReader(test.in))
	if err != nil {
		t.Errorf("valueTest error: %v", err)
	}
	if !eq(v, test.out) {
		t.Errorf("expected %q, got %q", test.out, v)
	}
}

var valueTests = []valueTest{
	{"+hello\r\n", String("hello")},
	{"+one two\r\n", String("one two")},   // intermediate space
	{"+one two \r\n", String("one two ")}, // trailing space
	{"+ one two\r\n", String(" one two")}, // leading space

	{"-hello\r\n", Error("hello")},
	{"-one two\r\n", Error("one two")},   // intermediate space
	{"-one two \r\n", Error("one two ")}, // trailing space
	{"- one two\r\n", Error(" one two")}, // leading space

	{"$-1\r\n", nil},
	{"$0\r\n\r\n", BulkString("")}, // is this even a thing?
	{"$1\r\nx\r\n", BulkString("x")},
	{"$4\r\netsy\r\n", BulkString("etsy")},
	{"$12\r\nSaskatchewan\r\n", BulkString("Saskatchewan")},

	{":0\r\n", Int(0)},
	{":1\r\n", Int(1)},
	{":-1\r\n", Int(-1)},
	{":12345\r\n", Int(12345)},
	{":-12345\r\n", Int(-12345)},
	{":9223372036854775807\r\n", Int(9223372036854775807)},   // int64 max
	{":-9223372036854775808\r\n", Int(-9223372036854775808)}, // int64 min

	{"+hello\r\n+extra\r\n", String("hello")},
	{"+one two\r\n+extra\r\n", String("one two")},   // intermediate space
	{"+one two \r\n+extra\r\n", String("one two ")}, // trailing space
	{"+ one two\r\n+extra\r\n", String(" one two")}, // leading space

	{"-hello\r\n+extra\r\n", Error("hello")},
	{"-one two\r\n+extra\r\n", Error("one two")},   // intermediate space
	{"-one two \r\n+extra\r\n", Error("one two ")}, // trailing space
	{"- one two\r\n+extra\r\n", Error(" one two")}, // leading space

	{":0\r\n+extra\r\n", Int(0)},
	{":1\r\n+extra\r\n", Int(1)},
	{":-1\r\n+extra\r\n", Int(-1)},
	{":12345\r\n+extra\r\n", Int(12345)},
	{":-12345\r\n+extra\r\n", Int(-12345)},
	{":9223372036854775807\r\n+extra\r\n", Int(9223372036854775807)},   // int64 max
	{":-9223372036854775808\r\n+extra\r\n", Int(-9223372036854775808)}, // int64 min

	{"*-1\r\n", nil}, // nil array
	// {"*0\r\n", Array{}}, // is this a thing?  I have no idea.
	// {"*1\r\n+hello\r\n", Array{String("hello")}},
	// {"*2\r\n+one\r\n+two", Array{String("one"), String("two")}},
	// {"*2\r\n$4\r\necho\r\n$5\r\nhello", Array{BulkString("echo"), BulkString("hello")}},
	// {"*2\r\n$4\r\necho\r\n$5\r\nhello\r\n+extra\r\n", Array{BulkString("echo"), BulkString("hello")}},
}

func TestValues(t *testing.T) {
	for _, test := range valueTests {
		test.run(t)
	}
}

type streamTest []interface{}

var streamTests = []streamTest{
	{"+hello\r\n", String("hello")},
	{":1\r\n:2\r\n:3\r\n", Int(1), Int(2), Int(3)},
	// {"*0\r\n", Array{}},
	// {"*1\r\n+one\r\n", Array{String("one")}},
	// {"*2\r\n+one\r\n+two\r\n", Array{String("one"), String("two")}},
	// {
	// 	"+preamble\r\n*2\r\n+one\r\n+two\r\n",
	// 	String("preamble"),
	// 	Array{String("one"), String("two")},
	// },
	// {
	// 	"+preamble\r\n*2\r\n+one\r\n+two\r\n+outro\r\n",
	// 	String("preamble"),
	// 	Array{String("one"), String("two")},
	// 	String("outro"),
	// },
	// {
	// 	"+preamble\r\n*2\r\n$3\r\none\r\n$3\r\ntwo\r\n+outro\r\n",
	// 	String("preamble"),
	// 	Array{BulkString("one"), BulkString("two")},
	// 	String("outro"),
	// },
	// {"-bad\r\n", Error("bad")},
}

func (s streamTest) run(t *testing.T) {
	in, out := s[0].(string), make([]value, len(s)-1)
	for i := 1; i < len(s); i++ {
		out[i-1] = s[i].(value)
	}

	c := make(chan maybe)
	go streamValues(strings.NewReader(in), c)
	var count int
	for v := range c {
		if v.ok() {
			if !eq(out[count], v.val()) {
				t.Errorf("expected %q, got %q", out[count], v.val())
			}
		} else {
			t.Error(v.err())
		}
		count++
	}
}

func TestStreams(t *testing.T) {
	for _, test := range streamTests {
		test.run(t)
	}
}
