package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/emirpasic/gods/trees/btree"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
)

const maxRetry = 100

func init() {
	f, err := os.OpenFile("/tmp/maelstrom.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
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
	tree    *btree.Tree
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

	if n.Parent != nil {
		neighbors = append(neighbors, n.Parent.Entries[0].Value.(string))
	}

	for _, children := range n.Children {
		for _, entry := range children.Entries {
			neighbors = append(neighbors, entry.Value.(string))
		}
	}

	for _, dst := range neighbors {
		if dst == src || dst == s.nodeID {
			continue
		}

		dst := dst
		go func() {
			if err := s.rpc(dst, body); err != nil {
				for i := 0; i < maxRetry; i++ {
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
	tree := btree.NewWithIntComparator(len(s.n.NodeIDs()))
	for i := 0; i < len(s.n.NodeIDs()); i++ {
		tree.Put(i, fmt.Sprintf("n%d", i))
	}

	s.nodesMu.Lock()
	s.tree = tree
	s.nodesMu.Unlock()

	return s.n.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
}

func id(s string) (int, error) {
	i, err := strconv.Atoi(s[1:])
	if err != nil {
		return 0, err
	}
	return i, nil
}
