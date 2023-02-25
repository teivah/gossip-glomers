package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

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
	br     *broadcaster

	idsMu sync.RWMutex
	ids   map[int]struct{}

	nodesMu   sync.RWMutex
	neighbors []string
}

func (s *server) initHandler(_ maelstrom.Message) error {
	s.nodeID = s.n.ID()
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
	neighbors := s.neighbors
	defer s.nodesMu.RUnlock()

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
	s.nodesMu.Lock()
	neighbors, err := topology(s.nodeID, s.n.NodeIDs())
	if err != nil {
		return err
	}
	s.neighbors = neighbors
	s.nodesMu.Unlock()

	return s.n.Reply(msg, map[string]any{
		"type": "topology_ok",
	})
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

func topology(sNodeID string, nodes []string) ([]string, error) {
	ids := make([]int, len(nodes))
	max := 0
	for i, node := range nodes {
		v, err := id(node)
		if err != nil {
			return nil, err
		}
		ids[i] = v
		if v > max {
			max = v
		}
	}

	nodeID, err := id(sNodeID)
	if err != nil {
		return nil, err
	}

	switch nodeID % 3 {
	case 0:
		return formatNodes(max, nodeID+3, nodeID-3, nodeID+1, nodeID+2), nil
	case 1:
		return formatNodes(max, nodeID+3, nodeID-3, nodeID-1, nodeID+12), nil
	case 2:
		return formatNodes(max, nodeID+3, nodeID-3, nodeID-2, nodeID-1), nil
	}
	return nil, nil
}

func formatNodes(max int, nodes ...int) []string {
	res := make([]string, 0, len(nodes))
	for _, node := range nodes {
		if node > max || node < 0 {
			continue
		}
		res = append(res, "n"+strconv.Itoa(node))
	}
	return res
}

func id(s string) (int, error) {
	i, err := strconv.Atoi(s[1:])
	if err != nil {
		return 0, err
	}
	return i, nil
}
