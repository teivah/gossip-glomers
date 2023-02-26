package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

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
	br := newBroadcaster(n, 10)
	defer br.close()
	s := &server{n: n, ids: make(map[int]struct{}), br: br}

	n.Handle("init", s.initHandler)
	n.Handle("broadcast", s.broadcastHandler)
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
	br     *broadcaster

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

func (s *server) broadcastHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	id := int(body["message"].(float64))
	s.idsMu.Lock()
	if _, exists := s.ids[id]; exists {
		s.idsMu.Unlock()
		return nil
	}
	s.ids[id] = struct{}{}
	s.idsMu.Unlock()

	if err := s.broadcast(msg.Src, body); err != nil {
		return err
	}

	return s.n.Reply(msg, map[string]any{
		"type": "broadcast_ok",
	})
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
	if v.level%2 == 0 {
		for _, sibling := range v.siblings {
			neighbors = append(neighbors, sibling.id)
		}
	}

	var exists []string
	if e, contains := body["exists"]; contains {
		s := e.(string)
		exists = strings.Split(s, ",")
	}
	exists = append(exists, s.nodeID)
	set := make(map[string]bool)
	for _, s := range exists {
		set[s] = true
	}

	var filtered []string
	for _, dst := range neighbors {
		if dst == src || dst == s.nodeID {
			continue
		}

		filtered = append(filtered, dst)
		exists = append(exists, dst)
	}
	body["exists"] = strings.Join(exists, ",")

	for _, dst := range neighbors {
		if set[dst] {
			continue
		}
		if dst == src || dst == s.nodeID {
			continue
		}

		s.br.broadcast(broadcastMsg{
			dst:  dst,
			body: body,
		})
	}
	return nil
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

func parseExists(exists string) map[string]bool {
	split := strings.Split(exists, ",")
	m := make(map[string]bool)
	for _, s := range split {
		m[s] = true
	}
	return m
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

type broadcastMsg struct {
	dst  string
	body map[string]any
}

type broadcaster struct {
	cancel context.CancelFunc
	ch     chan broadcastMsg
}

func newBroadcaster(n *maelstrom.Node, worker int) *broadcaster {
	ch := make(chan broadcastMsg)
	ctx, cancel := context.WithCancel(context.Background())

	for i := 0; i < worker; i++ {
		go func() {
			for {
				select {
				case msg := <-ch:
					for {
						if err := n.Send(msg.dst, msg.body); err != nil {
							continue
						}
						break
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	return &broadcaster{
		ch:     ch,
		cancel: cancel,
	}
}

func (b *broadcaster) broadcast(msg broadcastMsg) {
	b.ch <- msg
}

func (b *broadcaster) close() {
	b.cancel()
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
	}

	var q []entry
	q = append(q, entry{
		treeNode: v,
		node:     root,
	})

	level := -1
	for len(q) != 0 {
		count := len(q)
		var siblings []*node
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
				})
			}
		}

		for _, n := range siblings {
			n.siblings = siblings
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
