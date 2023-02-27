package main

import (
	"fmt"
	"testing"

	"github.com/emirpasic/gods/trees/btree"
)

func Test_server_topologyHandler(t *testing.T) {
	tree := btree.NewWithIntComparator(25)
	// TODO Use number of nodes
	for i := 0; i < 25; i++ {
		tree.Put(i, fmt.Sprintf("n%d", i))
	}

	fmt.Println(tree)

	i := 7

	n := tree.GetNode(i)
	//id := 11

	for id := 0; id < 25; id++ {

		var neighbors []string

		if n.Parent != nil {
			neighbors = append(neighbors, n.Parent.Entries[0].Value.(string))
		}

		for _, children := range n.Children {
			for _, entry := range children.Entries {
				s := entry.Value.(string)
				neighbors = append(neighbors, s)
			}
		}
		fmt.Println(neighbors)

	}

}
