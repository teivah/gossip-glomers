package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	rbt "github.com/emirpasic/gods/trees/redblacktree"
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
	tree    *rbt.Tree
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

func parseExists(exists string) map[string]bool {
	split := strings.Split(exists, ",")
	m := make(map[string]bool)
	for _, s := range split {
		m[s] = true
	}
	return m
}

func (s *server) broadcast(src string, body map[string]any) error {
	s.nodesMu.RLock()

	node := s.tree.GetNode(s.id)
	defer s.nodesMu.RUnlock()

	var neighbors []string
	if parent := node.Parent; parent != nil {
		neighbors = append(neighbors, fmt.Sprintf("%v", parent.Value))
	}
	if left := node.Left; left != nil {
		neighbors = append(neighbors, fmt.Sprintf("%v", left.Value))
	}
	if right := node.Right; right != nil {
		neighbors = append(neighbors, fmt.Sprintf("%v", right.Value))
	}

	log.Infof("src: %v, cur: %v => %v", src, s.nodeID, neighbors)

	for _, dst := range neighbors {
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

func (s *server) topologyHandler(msg maelstrom.Message) error {
	tree := rbt.NewWithIntComparator()
	// TODO Use number of nodes
	for i := 0; i < 25; i++ {
		tree.Put(i, fmt.Sprintf("n%d", i))
	}

	s.nodesMu.Lock()
	s.tree = tree
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
