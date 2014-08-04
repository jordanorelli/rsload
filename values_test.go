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
	if v != test.out {
		t.Errorf("expected %v, got %v", test.out, v)
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
}

func TestValues(t *testing.T) {
	for _, test := range valueTests {
		test.run(t)
	}
}
