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

const (
	batchFrequency = 500 * time.Millisecond
	maxRetry       = 100
)

func init() {
	f, err := os.OpenFile("/tmp/maelstrom.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	log.SetOutput(f)
}

func main() {
	n := maelstrom.NewNode()
	s := &server{n: n, ids: make(map[int]struct{}), broadcasts: make(map[string][]int)}

	n.Handle("init", s.initHandler)
	n.Handle("broadcast", s.broadcastHandler)
	n.Handle("read", s.readHandler)
	n.Handle("topology", s.topologyHandler)

	go func() {
		for {
			select {
			case <-time.After(batchFrequency):
				s.batchRPC()
			}
		}
	}()

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

	broadcastsMu sync.Mutex
	broadcasts   map[string][]int
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

	if _, contains := body["message"]; contains {
		message := int(body["message"].(float64))
		s.idsMu.Lock()
		if _, exists := s.ids[message]; exists {
			s.idsMu.Unlock()
			return nil
		}
		s.ids[message] = struct{}{}
		s.idsMu.Unlock()
		return s.broadcast(msg.Src, body)
	}

	// Batch message
	values := body["messages"].([]any)
	messages := make([]int, 0, len(values))
	s.idsMu.Lock()
	for _, v := range values {
		message := int(v.(float64))
		if _, exists := s.ids[message]; exists {
			continue
		}
		s.ids[message] = struct{}{}
		messages = append(messages, message)
	}
	s.idsMu.Unlock()
	return s.batchBroadcast(msg.Src, messages)
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

	message := int(body["message"].(float64))

	s.broadcastsMu.Lock()
	defer s.broadcastsMu.Unlock()
	for _, dst := range neighbors {
		if dst == src || dst == s.nodeID {
			continue
		}

		s.broadcasts[dst] = append(s.broadcasts[dst], message)
	}
	return nil
}

func (s *server) batchBroadcast(src string, messages []int) error {
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

	s.broadcastsMu.Lock()
	defer s.broadcastsMu.Unlock()
	for _, dst := range neighbors {
		if dst == src || dst == s.nodeID {
			continue
		}

		for _, message := range messages {
			s.broadcasts[dst] = append(s.broadcasts[dst], message)
		}
	}
	return nil
}

func (s *server) batchRPC() {
	s.broadcastsMu.Lock()
	defer s.broadcastsMu.Unlock()

	wg := sync.WaitGroup{}
	for dst, messages := range s.broadcasts {
		dst := dst
		messages := messages
		go func() {
			if err := s.rpcWithRetry(dst, map[string]any{
				"type":     "broadcast",
				"messages": messages,
			}, maxRetry); err != nil {
				log.Error(err)
			}
		}()
	}
	s.broadcasts = make(map[string][]int)
	wg.Wait()
}

func (s *server) rpcWithRetry(dst string, body map[string]any, retry int) error {
	var err error
	for i := 0; i < retry; i++ {
		if err = s.rpc(dst, body); err != nil {
			// Sleep and retry
			time.Sleep(100 * time.Duration(i) * time.Millisecond)
			continue
		}
		return nil
	}
	return err
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
