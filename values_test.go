package main

import (
	"testing"
)

var valueTests = []struct {
	in  string
	out value
}{
	{"+hello", simpleString("hello")},
	{"+one two", simpleString("one two")},   // intermediate space
	{"+one two ", simpleString("one two ")}, // trailing space
	{"+ one two", simpleString(" one two")}, // leading space

	{"-hello", redisError("hello")},
	{"-one two", redisError("one two")},   // intermediate space
	{"-one two ", redisError("one two ")}, // trailing space
	{"- one two", redisError(" one two")}, // leading space
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
