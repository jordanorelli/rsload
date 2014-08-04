package main

import (
	"testing"
)

var valueTests = []struct {
	in  string
	out value
}{
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
}

func TestValues(t *testing.T) {
	for _, test := range valueTests {
		v, err := readValue([]byte(test.in))
		if err != nil {
			t.Errorf("failed value test: %v", err)
		}
		if v != test.out {
			t.Errorf("expected %v, got %v", test.out, v)
		}
	}
}
