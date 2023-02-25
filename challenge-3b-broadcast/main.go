package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	s := &server{n: n, id: n.ID()}

	n.Handle("broadcast", s.broadcastHandler)
	n.Handle("read", s.readHandler)
	n.Handle("topology", s.topologyHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type server struct {
	n  *maelstrom.Node
	id string

	idsMu sync.RWMutex
	ids   []int

	nodesMu sync.RWMutex
	nodes   []string
}

func (s *server) broadcastHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.idsMu.Lock()
	s.ids = append(s.ids, int(body["message"].(float64)))
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
	nodes := make([]string, 0, len(s.nodes))
	copy(nodes, s.nodes)
	s.nodesMu.RUnlock()

	for _, dst := range nodes {
		if dst == src || dst == s.id {
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
	s.idsMu.RLock()
	ids := make([]int, len(s.ids))
	for i := 0; i < len(s.ids); i++ {
		ids[i] = s.ids[i]
	}
	s.idsMu.RUnlock()

	return s.n.Reply(msg, map[string]any{
		"type":     "read_ok",
		"messages": ids,
	})
}

func (s *server) topologyHandler(msg maelstrom.Message) error {
	s.nodesMu.Lock()
	s.nodes = s.n.NodeIDs()
	s.nodesMu.Unlock()

	return s.n.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
}
