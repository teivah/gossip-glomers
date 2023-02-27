package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	avl "github.com/emirpasic/gods/trees/avltree"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
)

func init() {
	f, err := os.OpenFile("/tmp/log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	log.SetOutput(f)
}

func main() {
	n := maelstrom.NewNode()
	s := &server{n: n, ids: make(map[int]struct{})}

	n.Handle("init", s.initHandler)
	n.Handle("broadcast", s.broadcastHandler)
	n.Handle("forward", s.forwardHandler)
	n.Handle("read", s.readHandler)
	n.Handle("topology", s.topologyHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type server struct {
	n      *maelstrom.Node
	nodeID string
	id     int

	idsMu sync.RWMutex
	ids   map[int]struct{}

	nodesMu sync.RWMutex
	tree    *avl.Tree
}

func (s *server) initHandler(_ maelstrom.Message) error {
	s.nodeID = s.n.ID()
	v, err := id(s.nodeID)
	if err != nil {
		return err
	}
	s.id = v
	return nil
}

/*
forward
message: 1000
to: n1
*/
func (s *server) forwardHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := s.n.SyncRPC(ctx, body["to"].(string), map[string]any{
		"type":    "broadcast",
		"message": int(body["message"].(float64)),
	})
	if err != nil {
		return err
	}

	return s.n.Reply(msg, map[string]any{
		"type": "forward_ok",
	})
}

func (s *server) broadcastHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	go func() {
		_ = s.n.Reply(msg, map[string]any{
			"type": "broadcast_ok",
		})
	}()

	id := int(body["message"].(float64))
	s.idsMu.Lock()
	if _, exists := s.ids[id]; exists {
		s.idsMu.Unlock()
		return nil
	}
	s.ids[id] = struct{}{}
	s.idsMu.Unlock()

	return s.broadcast(msg.Src, body)
}

func (s *server) broadcast(src string, body map[string]any) error {
	s.nodesMu.RLock()

	n := s.tree.GetNode(s.id)
	defer s.nodesMu.RUnlock()

	var neighbors []string
	if parent := n.Parent; parent != nil {
		v := parent.Value.(*node)
		neighbors = append(neighbors, v.id)
	}
	if left := n.Children[0]; left != nil {
		v := left.Value.(*node)
		neighbors = append(neighbors, v.id)
	}
	if right := n.Children[1]; right != nil {
		v := right.Value.(*node)
		neighbors = append(neighbors, v.id)
	}

	v := n.Value.(*node)
	if v.level >= 2 {
		if len(v.leftSiblings) != 0 {
			id := random(len(v.leftSiblings))
			neighbors = append(neighbors, v.leftSiblings[id].id)
		}

		if len(v.rightSiblings) != 0 {
			id := random(len(v.rightSiblings))
			neighbors = append(neighbors, v.rightSiblings[id].id)
		}

		//if v.rightSibling != nil {
		//	neighbors = append(neighbors, v.rightSibling.id)
		//}
		//if v.leftSibling != nil {
		//	neighbors = append(neighbors, v.leftSibling.id)
		//}

		//if v.leftMost {
		//	neighbors = append(neighbors, v.rightSibling.id)
		//}
		//if v.rightMost {
		//	neighbors = append(neighbors, v.leftSibling.id)
		//}

		//for _, sibling := range v.siblings {
		//	neighbors = append(neighbors, sibling.id)
		//}
		//if len(v.siblings) != 0 {
		//	neighbors = append(neighbors, v.siblings[0].id)
		//}
		//neighbors = append(neighbors, v.leftSibling.id)
		//neighbors = append(neighbors, v.rightSibling.id)

		//if v.leftMost {
		//	neighbors = append(neighbors, v.rightSibling.id)
		//} else if v.rightMost {
		//	neighbors = append(neighbors, v.leftSibling.id)
		//}
	}

	for _, dst := range neighbors {
		if dst == src || dst == s.nodeID {
			continue
		}

		dst := dst
		go func() {
			if err := s.rpc(dst, body); err != nil {
				for i := 0; i < 100; i++ {
					if err := s.rpc(dst, body); err != nil {
						// Sleep and retry
						time.Sleep(time.Duration(i) * time.Second)
						continue
					}
					return
				}
				log.Error(err)
			}
		}()
	}
	return nil
}

func random(max int) int {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	return r1.Int() % max
}

func (s *server) rpc(dst string, body map[string]any) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := s.n.SyncRPC(ctx, dst, body)
	return err
}

func (s *server) readHandler(msg maelstrom.Message) error {
	ids := s.getAllIDs()

	return s.n.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": ids,
	})
}

func (s *server) getAllIDs() []int {
	s.idsMu.RLock()
	ids := make([]int, 0, len(s.ids))
	for id := range s.ids {
		ids = append(ids, id)
	}
	s.idsMu.RUnlock()

	return ids
}

func (s *server) topologyHandler(msg maelstrom.Message) error {
	tree := avl.NewWithIntComparator()
	// TODO Use number of nodes
	for i := 0; i < 25; i++ {
		tree.Put(i, fmt.Sprintf("n%d", i))
	}
	root := toNode(tree)
	ftree := avl.NewWith(func(a, b interface{}) int {
		x := a.(int)
		y := b.(int)
		return x - y
	})
	dfs(ftree, root)

	//left := getLeftChild(ftree.Root)
	//right := getRightChild(ftree.Root)
	//setLeftMostChildren(ftree.Root.Children[0], right)
	//setRightChildren(ftree.Root.Children[1], left)

	//lefts := make(map[int]*node)
	//getLeftChildren(ftree.Root.Children[0], 1, lefts)
	//rights := make(map[int]*node)
	//getRightChildren(ftree.Root.Children[1], 1, rights)

	//setSiblings(ftree.Root.Children[0].Children[0], ftree.Root.Children[0].Children[1],
	//	ftree.Root.Children[1].Children[0], ftree.Root.Children[1].Children[1])

	lefts := make(map[int][]*node)
	getLevels(ftree.Root.Children[0], 1, lefts)

	rights := make(map[int][]*node)
	getLevels(ftree.Root.Children[1], 1, rights)

	for level, leftNodes := range lefts {
		rightNodes := rights[level]
		for _, n := range leftNodes {
			n.rightSiblings = rightNodes
		}
		for _, n := range rightNodes {
			n.leftSiblings = leftNodes
		}
	}

	//for level, n := range lefts {
	//	n.leftMost = true
	//	n.rightSibling = rights[level]
	//}
	//
	//for level, n := range rights {
	//	n.rightMost = true
	//	n.leftSibling = lefts[level]
	//}

	s.nodesMu.Lock()
	s.tree = ftree
	s.nodesMu.Unlock()

	return s.n.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
}

func (s *server) isComingFromUpwards(src string) (bool, error) {
	srcID, err := id(src)
	if err != nil {
		return false, err
	}

	cur, err := id(s.nodeID)
	if err != nil {
		return false, err
	}

	return srcID < cur, nil
}

func (s *server) isComingFromSameLevel(src string) (bool, error) {
	srcID, err := id(src)
	if err != nil {
		return false, err
	}
	cur, err := id(src)
	if err != nil {
		return false, err
	}

	return srcID%3 != cur%3, nil
}

func id(s string) (int, error) {
	i, err := strconv.Atoi(s[1:])
	if err != nil {
		return 0, err
	}
	return i, nil
}

type node struct {
	parent   *node
	left     *node
	right    *node
	level    int
	siblings []*node
	value    int
	id       string

	leftSibling   *node
	rightSibling  *node
	leftMost      bool
	rightMost     bool
	leftSiblings  []*node
	rightSiblings []*node
}

func toNode(tree *avl.Tree) *node {
	v := tree.GetNode(0)
	for v.Parent != nil {
		v = v.Parent
	}

	root := &node{value: v.Key.(int), id: fmt.Sprintf("%v", v.Value)}

	type entry struct {
		treeNode *avl.Node
		parent   *node
		node     *node
		left     bool
		right    bool
	}

	var q []entry
	q = append(q, entry{
		treeNode: v,
		node:     root,
		left:     true,
		right:    true,
	})

	level := -1
	for len(q) != 0 {
		count := len(q)
		var siblings []*node
		//var leftSibling *node
		//var rightSibling *node
		level++

		for i := 0; i < count; i++ {
			e := q[0]
			q = q[1:]

			if child := e.treeNode.Children[0]; child != nil {
				n := &node{
					parent: e.parent,
					value:  e.treeNode.Children[0].Key.(int),
					id:     fmt.Sprintf("%v", e.treeNode.Children[0].Value),
					level:  level,
				}
				siblings = append(siblings, n)

				e.node.left = n

				q = append(q, entry{
					treeNode: e.treeNode.Children[0],
					parent:   e.node,
					node:     n,
					left:     e.left,
				})
			}
			if child := e.treeNode.Children[1]; child != nil {
				n := &node{
					parent: e.parent,
					value:  e.treeNode.Children[1].Key.(int),
					id:     fmt.Sprintf("%v", e.treeNode.Children[1].Value),
					level:  level,
				}
				siblings = append(siblings, n)

				e.node.right = n

				q = append(q, entry{
					treeNode: e.treeNode.Children[1],
					parent:   e.node,
					node:     n,
					left:     e.right,
				})
			}
		}

		for _, n := range siblings {
			n.siblings = siblings
			//n.leftSibling = leftSibling
			//n.rightSibling = rightSibling
		}
	}

	return root
}

func dfs(ftree *avl.Tree, node *node) {
	if node == nil {
		return
	}

	ftree.Put(node.value, node)
	dfs(ftree, node.left)
	dfs(ftree, node.right)
}

func setSiblings(exteriorLeft, interiorLeft, interiorRight, exteriorRight *avl.Node) {
	if exteriorLeft == nil && interiorLeft == nil && interiorRight == nil && exteriorRight == nil {
		return
	}

	if exteriorLeft != nil && exteriorRight != nil {
		l := exteriorLeft.Value.(*node)
		r := exteriorRight.Value.(*node)
		l.rightSibling = r
		r.leftSibling = l

		exteriorLeft = exteriorLeft.Children[0]
		exteriorRight = exteriorRight.Children[1]
	} else {
		exteriorLeft = nil
		exteriorRight = nil
	}

	if interiorLeft != nil && interiorRight != nil {
		l := interiorLeft.Value.(*node)
		r := interiorRight.Value.(*node)
		l.rightSibling = r
		r.leftSibling = l

		interiorLeft = interiorLeft.Children[1]
		interiorRight = interiorRight.Children[0]
	} else {
		interiorLeft = nil
		interiorRight = nil
	}

	setSiblings(exteriorLeft, interiorLeft, interiorRight, exteriorRight)
}

func getLevels(n *avl.Node, level int, levels map[int][]*node) {
	if n == nil {
		return
	}

	v := n.Value.(*node)
	levels[level] = append(levels[level], v)

	getLevels(n.Children[0], level+1, levels)
	getLevels(n.Children[1], level+1, levels)
}

func getLeftChildren(n *avl.Node, level int, levels map[int]*node) {
	if n == nil {
		return
	}
	v := n.Value.(*node)
	levels[level] = v
	getLeftChildren(n.Children[0], level+1, levels)
}

func getRightChildren(n *avl.Node, level int, levels map[int]*node) {
	if n == nil {
		return
	}
	v := n.Value.(*node)
	levels[level] = v
	getRightChildren(n.Children[1], level+1, levels)
}
