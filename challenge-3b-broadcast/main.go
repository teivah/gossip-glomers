package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	s := &server{n: n, nodeID: n.ID(), ids: make(map[int]struct{})}

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

	idsMu sync.RWMutex
	ids   map[int]struct{}
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
	for _, dst := range s.n.NodeIDs() {
		if dst == src || dst == s.nodeID {
			continue
		}

		dst := dst
		go func() {
			if err := s.n.Send(dst, body); err != nil {
				panic(err)
			}
		}()
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
	return s.n.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
}
