package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	log "github.com/sirupsen/logrus"
)

const (
	prefixCommit = "commit_"
	prefixLatest = "latest_"
	prefixEntry  = "entry_"
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
	kv := maelstrom.NewLinKV(n)
	s := &server{
		n:  n,
		kv: kv,
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
	kv     *maelstrom.KV
	nodeID string
	id     int
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

	keyLatest := fmt.Sprintf("%s%s", prefixLatest, key)
	offset, err := s.kv.ReadInt(context.Background(), keyLatest)
	if err != nil {
		rpcErr := err.(*maelstrom.RPCError)
		if rpcErr.Code == maelstrom.KeyDoesNotExist {
			offset = 0
		} else {
			return err
		}
	} else {
		offset++
	}

	for ; ; offset++ {
		if err := s.kv.CompareAndSwap(context.Background(),
			keyLatest, offset-1, offset, true); err != nil {
			log.Warnf("cas retry: %v", err)
			continue
		}
		break
	}

	if err := s.kv.Write(context.Background(), fmt.Sprintf("%s%s_%d", prefixEntry, key, offset), message); err != nil {
		return err
	}

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
	for key, startingOffset := range req.Offsets {
		for offset := startingOffset; ; offset++ {
			message, exists, err := s.getValue(key, offset)
			if err != nil {
				return err
			}
			if !exists {
				break
			}
			res[key] = append(res[key], [2]int{offset, message})
		}
	}

	return s.n.Reply(msg, map[string]any{
		"type": "poll_ok",
		"msgs": res,
	})
}

func (s *server) getValue(key string, offset int) (int, bool, error) {
	v, err := s.kv.ReadInt(context.Background(), fmt.Sprintf("%s%s_%d", prefixEntry, key, offset))
	if err != nil {
		rpcErr := err.(*maelstrom.RPCError)
		if rpcErr.Code == maelstrom.KeyDoesNotExist {
			return 0, false, nil
		}
		return 0, false, err
	}
	return v, true, nil
}

func (s *server) commitHandler(msg maelstrom.Message) error {
	var req offsetReq
	if err := json.Unmarshal(msg.Body, &req); err != nil {
		return err
	}

	for key, offset := range req.Offsets {
		if err := s.kv.Write(context.Background(), fmt.Sprintf("%s%s", prefixCommit, key), offset); err != nil {
			return err
		}
	}

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
	for _, key := range keys {
		k := key.(string)

		v, err := s.kv.ReadInt(context.Background(), fmt.Sprintf("%s%s", prefixCommit, k))
		if err != nil {
			v = 0
		}

		res[k] = v
	}

	return s.n.Reply(msg, map[string]any{
		"type":    "list_committed_offsets_ok",
		"offsets": res,
	})
}
