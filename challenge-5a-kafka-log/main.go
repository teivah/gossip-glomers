package main

import (
	"encoding/json"
	"os"
	"strconv"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
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
	s := &server{
		n:                n,
		logs:             make(map[string][]entry),
		committedOffsets: make(map[string]int),
		latestOffsets:    make(map[string]int),
	}

	n.Handle("init", wrap(s.initHandler))
	n.Handle("send", wrap(s.sendHandler))
	n.Handle("poll", wrap(s.pollHandler))
	n.Handle("commit_offsets", wrap(s.commitHandler))
	n.Handle("list_committed_offsets", wrap(s.listHandler))

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

func wrap(f func(msg maelstrom.Message) error) func(msg maelstrom.Message) error {
	return func(msg maelstrom.Message) error {
		if err := f(msg); err != nil {
			log.Error(err)
			return err
		}
		return nil
	}
}

type server struct {
	n      *maelstrom.Node
	nodeID string
	id     int

	mu               sync.RWMutex
	logs             map[string][]entry
	committedOffsets map[string]int
	latestOffsets    map[string]int
}

type entry struct {
	offset  int
	message int
}

func (s *server) initHandler(_ maelstrom.Message) error {
	s.nodeID = s.n.ID()
	id, err := strconv.Atoi(s.nodeID[1:])
	if err != nil {
		return err
	}
	s.id = id
	return nil
}

func (s *server) sendHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	key := body["key"].(string)
	message := int(body["msg"].(float64))

	s.mu.Lock()
	offset := s.latestOffsets[key] + 1
	s.logs[key] = append(s.logs[key], entry{
		offset:  offset,
		message: message,
	})
	s.latestOffsets[key] = offset
	s.mu.Unlock()

	return s.n.Reply(msg, map[string]any{
		"type":   "send_ok",
		"offset": offset,
	})
}

type offsetReq struct {
	Offsets map[string]int `json:"offsets"`
}

func (s *server) pollHandler(msg maelstrom.Message) error {
	var req offsetReq
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	res := make(map[string][][2]int)

	s.mu.Lock()
	for key, startingOffset := range req.Offsets {
		entries := s.logs[key]
		i := findOffset(entries, startingOffset)
		for ; i < len(entries); i++ {
			e := entries[i]
			res[key] = append(res[key], [2]int{e.offset, e.message})
		}
	}
	s.mu.Unlock()

	return s.n.Reply(msg, map[string]any{
		"type": "poll_ok",
		"msgs": res,
	})
}

func findOffset(entries []entry, startingOffset int) int {
	l := 0
	r := len(entries) - 1
	for l <= r {
		mid := l + (r-l)/2
		n := entries[mid].offset
		if n == startingOffset {
			return mid
		}
		if n < startingOffset {
			l = mid + 1
		} else {
			r = mid - 1
		}
	}
	if l == 0 {
		return 0
	}
	return l
}

func (s *server) commitHandler(msg maelstrom.Message) error {
	var req offsetReq
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	s.mu.Lock()
	for key, offset := range req.Offsets {
		s.committedOffsets[key] = offset
	}
	s.mu.Unlock()

	return s.n.Reply(msg, map[string]any{
		"type": "commit_offsets_ok",
	})
}

func (s *server) listHandler(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	res := make(map[string]int)

	keys := body["keys"].([]any)
	s.mu.RLock()
	for _, key := range keys {
		k := key.(string)
		res[k] = s.committedOffsets[k]
	}
	s.mu.RUnlock()

	return s.n.Reply(msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": res,
	})
}
