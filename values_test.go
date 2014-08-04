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
