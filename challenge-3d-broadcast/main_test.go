package main

import (
	"fmt"
	"testing"

	rbt "github.com/emirpasic/gods/trees/redblacktree"
)

func Test_server_topologyHandler(t *testing.T) {
	tree := rbt.NewWithIntComparator()
	// TODO Use number of nodes
	for i := 0; i < 25; i++ {
		tree.Put(i, fmt.Sprintf("n%d", i))
	}

	n := tree.GetNode(0)
	fmt.Println(n.Value)
	fmt.Println(n.Left)
	fmt.Println(n.Right)
	fmt.Println(n.Parent)
}
