package main

import (
	"fmt"
	"testing"
)

func Test_parse(t *testing.T) {
	entries := []entry{
		{32, 21},
		{123, 3302},
	}

	l := logEntries{
		entries: entries,
	}
	s := l.String()

	l2, err := toLogEntries(s)
	if err != nil {
		panic(l2)
	}

	fmt.Println(s)
	fmt.Println(l2)
}

func Test_toLogEntries(t *testing.T) {
	fmt.Println(toLogEntries(""))
}
