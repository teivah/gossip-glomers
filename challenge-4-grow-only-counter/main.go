package main

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
)

const defaultTimeout = time.Second

func init() {
	f, err := os.OpenFile("/tmp/maelstrom.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	log.SetOutput(f)
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	s := &server{n: n, kv: kv, cache: make(map[string]int)}

	n.Handle("init", s.initHandler)
	n.Handle("add", s.addHandler)
	n.Handle("read", s.readHandler)
	n.Handle("local", s.localHandler)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

type server struct {
	n      *maelstrom.Node
	nodeID string
	id     int
	kv     *maelstrom.KV
	mu     sync.Mutex
	cache  map[string]int
}

func (s *server) initHandler(_ maelstrom.Message) error {
	s.mu.Lock()
	s.nodeID = s.n.ID()
	id, err := strconv.Atoi(s.nodeID[1:])
	if err != nil {
		return err
	}
	s.id = id

	defer s.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	if err := s.kv.Write(ctx, s.nodeID, 0); err != nil {
		log.Error(err)
		return err
	}
	return nil
}

func (s *server) addHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	delta := int(body["delta"].(float64))

	s.mu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	sum, err := s.kv.ReadInt(ctx, s.nodeID)
	if err != nil {
		return err
	}

	ctx, cancel2 := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel2()
	err = s.kv.Write(ctx, s.nodeID, sum+delta)
	s.mu.Unlock()
	if err != nil {
		log.Error(err)
		return err
	}

	return s.n.Reply(msg, map[string]any{
		"type": "add_ok",
	})
}

func (s *server) readHandler(msg maelstrom.Message) error {
	sum := 0
	for _, nodeID := range s.n.NodeIDs() {
		if nodeID == s.nodeID {
			ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			v, err := s.kv.ReadInt(ctx, s.nodeID)
			cancel()
			if err != nil {
				log.Warnf("failed to read %s: %v", s.nodeID, err)
				// Default to local cache
				sum += s.cache[nodeID]
				continue
			}

			sum += v
			s.cache[nodeID] = v
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
			res, err := s.n.SyncRPC(ctx, nodeID, map[string]any{
				"type": "local",
			})
			cancel()
			if err != nil {
				log.Warnf("failed to call local endpoint %s from %s: %v", nodeID, s.nodeID, err)
				// Default to local cache
				sum += s.cache[nodeID]
				continue
			}

			var body map[string]any
			if err := json.Unmarshal(res.Body, &body); err != nil {
				return err
			}

			v := int(body["value"].(float64))
			sum += v
			s.cache[nodeID] = v
		}
	}

	return s.n.Reply(msg, map[string]any{
		"type":  "read_ok",
		"value": sum,
	})
}

func (s *server) localHandler(msg maelstrom.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	defer cancel()
	v, err := s.kv.ReadInt(ctx, s.nodeID)
	if err != nil {
		return err
	}

	return s.n.Reply(msg, map[string]any{
		"type":  "local_ok",
		"value": v,
	})
}
