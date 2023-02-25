package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	s := &server{n: n}

	n.Handle("broadcast", s.broadcastHandler)
	n.Handle("read", s.readHandler)
	n.Handle("topology", s.topologyHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type server struct {
	n *maelstrom.Node

	idsMu sync.RWMutex
	ids   []int

	topologyMu      sync.RWMutex
	currentTopology map[string][]string
}

func (s *server) broadcastHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	s.idsMu.Lock()
	s.ids = append(s.ids, int(body["message"].(float64)))
	s.idsMu.Unlock()

	return s.n.Reply(msg, map[string]any{
		"type": "broadcast_ok",
	})
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

type topologyMsg struct {
	Topology map[string][]string `json:"topology"`
}

func (s *server) topologyHandler(msg maelstrom.Message) error {
	var t topologyMsg
	if err := json.Unmarshal(msg.Body, &t); err != nil {
		return err
	}

	s.topologyMu.RLock()
	s.currentTopology = t.Topology
	s.topologyMu.RUnlock()

	return s.n.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
}
