package main

import (
	"fmt"
	"testing"
)

func Test_topology(t *testing.T) {
	fmt.Println(topology("n4", []string{"n0", "n1", "n2", "n3", "n4"}))
}
