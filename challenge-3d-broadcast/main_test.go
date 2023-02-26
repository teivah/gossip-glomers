package main

import (
	"fmt"
	"testing"

	avl "github.com/emirpasic/gods/trees/avltree"
)

func Test_server_topologyHandler(t *testing.T) {
	tree := avl.NewWithIntComparator()
	// TODO Use number of nodes
	for i := 0; i < 25; i++ {
		tree.Put(i, fmt.Sprintf("n%d", i))
	}

	root := toNode(tree)
	ftree := avl.NewWith(func(a, b interface{}) int {
		x := a.(int)
		y := b.(int)
		return y - x
	})
	dfs(ftree, root)
	fmt.Println(ftree)
}
