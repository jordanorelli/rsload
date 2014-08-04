package main

import (
	"strings"
	"testing"
)

type valueTest struct {
	in  string
	out value
}

func (test valueTest) run(t *testing.T) {
	v, err := readValue(strings.NewReader(test.in + "\r\n"))
	if err != nil {
		t.Errorf("valueTest error: %v", err)
	}
	switch expected := test.out.(type) {
	case Array:
		got, ok := v.(Array)
		if !ok {
			t.Errorf("expected Array value, got %v", v)
		}
		if len(got) != len(expected) {
			t.Errorf("expected Array of length %d, saw Array of length %d", len(expected), len(got))
		}
		for i := 0; i < len(got); i++ {
			if got[i] != expected[i] {
				t.Errorf("Array values do not match: got %v, expected %v", got, expected)
			}
		}
	default:
		if v != test.out {
			t.Errorf("expected %v, got %v", test.out, v)
		}
	}
}

var valueTests = []valueTest{
	{"+hello", String("hello")},
	{"+one two", String("one two")},   // intermediate space
	{"+one two ", String("one two ")}, // trailing space
	{"+ one two", String(" one two")}, // leading space

	{"-hello", Error("hello")},
	{"-one two", Error("one two")},   // intermediate space
	{"-one two ", Error("one two ")}, // trailing space
	{"- one two", Error(" one two")}, // leading space

	{"$-1\r\n", nil},
	{"$0\r\n\r\n", BulkString("")}, // is this even a thing?
	{"$1\r\nx\r\n", BulkString("x")},
	{"$4\r\netsy\r\n", BulkString("etsy")},
	{"$12\r\nSaskatchewan\r\n", BulkString("Saskatchewan")},

	{":0", Integer(0)},
	{":1", Integer(1)},
	{":-1", Integer(-1)},
	{":12345", Integer(12345)},
	{":-12345", Integer(-12345)},
	{":9223372036854775807", Integer(9223372036854775807)},   // int64 max
	{":-9223372036854775808", Integer(-9223372036854775808)}, // int64 min

	{"+hello\r\n+extra\r\n", String("hello")},
	{"+one two\r\n+extra\r\n", String("one two")},   // intermediate space
	{"+one two \r\n+extra\r\n", String("one two ")}, // trailing space
	{"+ one two\r\n+extra\r\n", String(" one two")}, // leading space

	{"-hello\r\n+extra\r\n", Error("hello")},
	{"-one two\r\n+extra\r\n", Error("one two")},   // intermediate space
	{"-one two \r\n+extra\r\n", Error("one two ")}, // trailing space
	{"- one two\r\n+extra\r\n", Error(" one two")}, // leading space

	{":0\r\n+extra\r\n", Integer(0)},
	{":1\r\n+extra\r\n", Integer(1)},
	{":-1\r\n+extra\r\n", Integer(-1)},
	{":12345\r\n+extra\r\n", Integer(12345)},
	{":-12345\r\n+extra\r\n", Integer(-12345)},
	{":9223372036854775807\r\n+extra\r\n", Integer(9223372036854775807)},   // int64 max
	{":-9223372036854775808\r\n+extra\r\n", Integer(-9223372036854775808)}, // int64 min

	{"*0\r\n", Array{}}, // is this a thing?  I have no idea.
	{"*1\r\n+hello\r\n", Array{String("hello")}},
	{"*2\r\n+one\r\n+two", Array{String("one"), String("two")}},
	{"*2\r\n$4\r\necho\r\n$5\r\nhello", Array{BulkString("echo"), BulkString("hello")}},
}

func TestValues(t *testing.T) {
	for _, test := range valueTests {
		test.run(t)
	}
}
