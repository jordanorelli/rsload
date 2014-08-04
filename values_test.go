package main

import (
    "testing"
)

func TestSimpleString(t *testing.T) {
    s, err := readValue([]byte(`+hello`))
    if err != nil {
        t.Errorf("bad input: %v", err)
    }
    if s != simpleString("hello") {
        t.Errorf("expected 'hello', got '%s'", s)
    }
}

